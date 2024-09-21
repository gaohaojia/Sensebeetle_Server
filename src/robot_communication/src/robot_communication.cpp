#include <arpa/inet.h>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <functional>
#include <geometry_msgs/msg/detail/point_stamped__struct.hpp>
#include <geometry_msgs/msg/detail/transform_stamped__struct.hpp>
#include <geometry_msgs/msg/pose_stamped.hpp>
#include <memory>
#include <netinet/in.h>
#include <pcl/common/transforms.h>
#include <pcl_conversions/pcl_conversions.h>
#include <rclcpp/logging.hpp>
#include <rclcpp/qos.hpp>
#include <rclcpp/serialization.hpp>
#include <rclcpp/serialized_message.hpp>
#include <rclcpp/utilities.hpp>
#include <rmw/qos_profiles.h>
#include <rmw/types.h>
#include <sensor_msgs/msg/detail/image__struct.hpp>
#include <sensor_msgs/msg/detail/point_cloud2__struct.hpp>
#include <string>
#include <sys/socket.h>
#include <tf2/LinearMath/Quaternion.h>
#include <tf2/LinearMath/Transform.h>
#include <tf2/LinearMath/Vector3.h>
#include <tf2/convert.h>
#include <tf2/time.h>
#include <tf2/transform_datatypes.h>
#include <tf2_eigen/tf2_eigen.hpp>
#include <tf2_geometry_msgs/tf2_geometry_msgs.hpp>
#include <thread>
#include <vector>

#include "robot_communication/robot_communication.hpp"

#define MAX_PACKET_SIZE 64000
#define BUFFER_SIZE 65535
#define MAX_BUFFER_QUEUE_SIZE 256

namespace robot_communication
{
RobotCommunicationNode::RobotCommunicationNode(const rclcpp::NodeOptions & options)
  : Node("robot_communication", options)
{
  this->declare_parameter<int>("robot_count", 3);
  this->declare_parameter<int>("network_port", 12130);
  this->declare_parameter<std::string>("network_ip", "192.168.31.207");

  this->get_parameter("robot_count", robot_count);
  this->get_parameter("network_port", port);
  this->get_parameter("network_ip", ip);

  for (int i = 0; i < robot_count; i++) {
    registered_scan_pub_[i] = this->create_publisher<sensor_msgs::msg::PointCloud2>(
      "/robot_" + std::to_string(i) + "/total_registered_scan", 5);
    realsense_image_pub_[i] = this->create_publisher<sensor_msgs::msg::Image>(
      "/robot_" + std::to_string(i) + "/image_raw", 5);
    way_point_sub_[i] = this->create_subscription<geometry_msgs::msg::PointStamped>(
      "/robot_" + std::to_string(i) + "/way_point",
      2,
      [this, i](const geometry_msgs::msg::PointStamped::SharedPtr msg) {
        WayPointCallBack(msg, i);
      });
  }

  InitServer();
}

RobotCommunicationNode::~RobotCommunicationNode()
{
  if (send_thread_.joinable()) {
    send_thread_.join();
  }
  if (recv_thread_.joinable()) {
    recv_thread_.join();
  }
  if (prepare_buffer_thread_.joinable()) {
    prepare_buffer_thread_.join();
  }
  for (int i = 0; i < robot_count; i++) {
    if (parse_buffer_thread_[i].joinable()) {
      parse_buffer_thread_[i].join();
    }
  }
  close(sockfd);
}

// initialize the socket server
void RobotCommunicationNode::InitServer()
{
  if ((sockfd = socket(AF_INET, SOCK_DGRAM, 0)) < 0) {
    RCLCPP_ERROR(this->get_logger(), "Socket creation failed!");
    return;
  }

  memset(&server_addr, 0, sizeof(server_addr));
  memset(&saved_client_addr, 0, sizeof(saved_client_addr));

  server_addr.sin_family = AF_INET;
  server_addr.sin_port = htons(port);
  server_addr.sin_addr.s_addr = inet_addr(ip.c_str());

  if (bind(sockfd, (const struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
    RCLCPP_ERROR(this->get_logger(), "Bind failed!");
    close(sockfd);
    return;
  }

  send_thread_ = std::thread(&RobotCommunicationNode::NetworkSendThread, this);
  for (int i = 0; i < robot_count; i++) {
    parse_buffer_thread_[i] = std::thread(&RobotCommunicationNode::ParseBufferThread, this, i);
  }
  prepare_buffer_thread_ = std::thread(&RobotCommunicationNode::PrepareBufferThread, this);
  recv_thread_ = std::thread(&RobotCommunicationNode::NetworkRecvThread, this);
  RCLCPP_INFO(this->get_logger(), "Server start at ip: %s, port: %d", ip.c_str(), port);
}

void RobotCommunicationNode::WayPointCallBack(
  const geometry_msgs::msg::PointStamped::ConstSharedPtr way_point_msg, const int robot_id)
{
  std::vector<uint8_t> data_buffer = SerializeMsg<geometry_msgs::msg::PointStamped>(*way_point_msg);
  PrepareBuffer pthread_buffer = {robot_id, data_buffer, 0};
  if (prepare_buffer_queue.size() >= MAX_BUFFER_QUEUE_SIZE) {
    return;
  }
  prepare_buffer_queue.push(pthread_buffer);
}

void RobotCommunicationNode::NetworkSendThread()
{
  while (rclcpp::ok()) {
    if (buffer_queue.empty()) {
      std::this_thread::sleep_for(std::chrono::nanoseconds(10));
      continue;
    }
    SendBuffer s_buffer = buffer_queue.front();
    buffer_queue.pop();
    if (saved_client_addr[s_buffer.id].sin_addr.s_addr == 0) {
      continue;
    }
    if (sendto(sockfd,
               s_buffer.buffer.data(),
               s_buffer.buffer.size(),
               0,
               (const struct sockaddr *)&saved_client_addr[s_buffer.id],
               sizeof(saved_client_addr[s_buffer.id])) < 0) {
      RCLCPP_ERROR(this->get_logger(), "Send failed!");
    }
  }
}

void RobotCommunicationNode::NetworkRecvThread()
{
  struct sockaddr_in client_addr;
  memset(&client_addr, 0, sizeof(client_addr));
  int n, len = sizeof(client_addr);
  while (rclcpp::ok()) {
    std::vector<uint8_t> buffer_tmp(BUFFER_SIZE);
    n = recvfrom(sockfd,
                 buffer_tmp.data(),
                 BUFFER_SIZE,
                 MSG_WAITFORONE,
                 (struct sockaddr *)&client_addr,
                 (socklen_t *)&len);
    if (n < 0) {
      continue;
    }
    buffer_tmp.resize(n);

    uint8_t id;
    std::memcpy(&id, buffer_tmp.data(), sizeof(id));
    saved_client_addr[id] = client_addr;
    if (parse_buffer_queue[id].size() >= MAX_BUFFER_QUEUE_SIZE) {
      continue;
    }
    parse_buffer_queue[id].push(buffer_tmp);
  }
}

void RobotCommunicationNode::PrepareBufferThread()
{
  while (rclcpp::ok()) {
    if (prepare_buffer_queue.empty()) {
      std::this_thread::sleep_for(std::chrono::nanoseconds(10));
      continue;
    }
    PrepareBuffer pthread_buffer = prepare_buffer_queue.front();
    prepare_buffer_queue.pop();
    const int total_packet = (pthread_buffer.buffer.size() + MAX_PACKET_SIZE - 1) / MAX_PACKET_SIZE;
    if (prepare_buffer_queue.size() >= MAX_BUFFER_QUEUE_SIZE) {
      return;
    }
    for (int i = 0; i < total_packet; i++) {
      uint8_t id = pthread_buffer.id;
      uint8_t type = pthread_buffer.msg_type;
      uint16_t idx = i;
      uint8_t max_idx = total_packet;
      std::vector<uint8_t> header(sizeof(uint32_t) + sizeof(uint8_t));
      std::memcpy(header.data(), &id, sizeof(id));
      std::memcpy(header.data() + sizeof(uint8_t), &type, sizeof(type));
      std::memcpy(header.data() + sizeof(uint16_t), &idx, sizeof(idx));
      std::memcpy(header.data() + sizeof(uint32_t), &max_idx, sizeof(max_idx));
      std::vector<uint8_t> packet;
      packet.insert(packet.end(), header.begin(), header.end());
      packet.insert(packet.end(),
                    pthread_buffer.buffer.begin() + i * MAX_PACKET_SIZE,
                    i == total_packet - 1 ? pthread_buffer.buffer.end()
                                          : pthread_buffer.buffer.begin() + (i + 1) * MAX_PACKET_SIZE);
      SendBuffer s_buffer;
      s_buffer.buffer = packet;
      s_buffer.id = pthread_buffer.id;
      buffer_queue.push(s_buffer);
    }
  }
}

void RobotCommunicationNode::ParseBufferThread(const int robot_id)
{
  int packet_idx = 0;
  int packet_type = -1;
  std::vector<uint8_t> buffer;
  while (rclcpp::ok()) {
    if (parse_buffer_queue[robot_id].empty()) {
      std::this_thread::sleep_for(std::chrono::nanoseconds(10));
      continue;
    }
    std::vector<uint8_t> buffer_tmp = parse_buffer_queue[robot_id].front();
    parse_buffer_queue[robot_id].pop();

    uint8_t id, type, max_idx;
    uint16_t idx;
    std::memcpy(&id, buffer_tmp.data(), sizeof(id));
    std::memcpy(&type, buffer_tmp.data() + sizeof(uint8_t), sizeof(type));
    std::memcpy(&idx, buffer_tmp.data() + sizeof(uint16_t), sizeof(idx));
    std::memcpy(&max_idx, buffer_tmp.data() + sizeof(uint32_t), sizeof(max_idx));

    if (packet_type == -1) {
      packet_type = type;
    } else if (packet_type < type) {
      continue;
    } else if (packet_type > type) {
      packet_type = type;
      packet_idx = 0;
      buffer = std::vector<uint8_t>(0);
    }
    if (idx == 0) {
      packet_idx = 0;
      buffer = std::vector<uint8_t>(0);
    } else if (packet_idx != idx) {
      packet_idx = 0;
      packet_type = -1;
      buffer = std::vector<uint8_t>(0);
      continue;
    }

    packet_idx++;
    if (packet_idx == 1) {
      buffer.insert(
        buffer.begin(), buffer_tmp.begin() + sizeof(uint32_t) + sizeof(uint8_t), buffer_tmp.end());
    } else {
      buffer.insert(
        buffer.end(), buffer_tmp.begin() + sizeof(uint32_t) + sizeof(uint8_t), buffer_tmp.end());
    }

    if (packet_idx != max_idx) {
      continue;
    }
    try {
      if (type == 0) { // PointCloud2
        sensor_msgs::msg::PointCloud2 totalRegisteredScan =
          DeserializeMsg<sensor_msgs::msg::PointCloud2>(buffer);
        registered_scan_pub_[id]->publish(totalRegisteredScan);
      } else if (type == 1) { // Image
        sensor_msgs::msg::Image realsense_image = DeserializeMsg<sensor_msgs::msg::Image>(buffer);
        realsense_image_pub_[id]->publish(realsense_image);
      } else if (type == 2) { // Transform
        geometry_msgs::msg::TransformStamped transformStamped =
          DeserializeMsg<geometry_msgs::msg::TransformStamped>(buffer);
        tf2_ros::TransformBroadcaster tf_broadcaster_ = this;
        tf_broadcaster_.sendTransform(transformStamped);
      }
    } catch (...) {
    }

    packet_idx = 0;
    packet_type = -1;
    buffer = std::vector<uint8_t>(0);
  }
}

// Serialization
template <class T> std::vector<uint8_t> RobotCommunicationNode::SerializeMsg(const T & msg)
{
  rclcpp::SerializedMessage serialized_msg;
  rclcpp::Serialization<T> serializer;
  serializer.serialize_message(&msg, &serialized_msg);

  std::vector<uint8_t> buffer_tmp(serialized_msg.size());
  std::memcpy(
    buffer_tmp.data(), serialized_msg.get_rcl_serialized_message().buffer, serialized_msg.size());

  return buffer_tmp;
}

// Deserialization
template <class T> T RobotCommunicationNode::DeserializeMsg(const std::vector<uint8_t> & data)
{
  rclcpp::SerializedMessage serialized_msg;
  rclcpp::Serialization<T> serializer;

  serialized_msg.reserve(data.size());
  std::memcpy(serialized_msg.get_rcl_serialized_message().buffer, data.data(), data.size());
  serialized_msg.get_rcl_serialized_message().buffer_length = data.size();

  T msg;
  serializer.deserialize_message(&serialized_msg, &msg);

  return msg;
}
} // namespace robot_communication

#include "rclcpp_components/register_node_macro.hpp"

// Register the component with class_loader.
// This acts as a sort of entry point, allowing the component to be discoverable when its library
// is being loaded into a running process.
RCLCPP_COMPONENTS_REGISTER_NODE(robot_communication::RobotCommunicationNode)