import os

from launch import LaunchDescription
from launch_ros.actions import Node

from ament_index_python.packages import get_package_share_directory

def generate_launch_description():

    package_name = "prod_line_plc_comm"

    params_file = os.path.join(
        get_package_share_directory(package_name),
        "params",
        "plc_config.yaml"
    )
    
    ld = LaunchDescription()
    
    node = Node(
        package=package_name,
        executable="plc_comm_node",
        name="plc_comm_node",
        output="screen",
        parameters=[params_file]
    )

    ld.add_action(node)

    return ld