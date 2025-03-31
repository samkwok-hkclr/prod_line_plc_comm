import sys
import asyncio
from asyncio import run_coroutine_threadsafe
from threading import Thread, Lock
from typing import Optional

import rclpy

from rclpy.node import Node
from rclpy.callback_groups import MutuallyExclusiveCallbackGroup, ReentrantCallbackGroup
from rclpy.executors import ExternalShutdownException, MultiThreadedExecutor

from pymodbus.client import AsyncModbusTcpClient as Client
from pymodbus.pdu.register_message import ReadHoldingRegistersResponse, WriteMultipleRegistersResponse

from std_msgs.msg import Bool, UInt8, UInt8MultiArray
from smdps_msgs.srv import ReadRegister, WriteRegister

from .req_temp import RequestTemplate as ReqTemp

class PlcComm(Node):
    def __init__(self):
        super().__init__("plc_comm")
        self.declare_parameter("ip", "127.0.0.1")
        self.declare_parameter("port", 502)

        self.ip = self.get_parameter("ip").value
        self.port = self.get_parameter("port").value

        # Modbus TCP Client
        self.cli = None

        # asyncio event loop
        self.loop = asyncio.new_event_loop()
        self.loop_thread = Thread(target=self.run_loop, daemon=True)
        self.loop_thread.start()

        # Mutex
        self.mutex = Lock()

        # Callback groups
        srv_ser_read_cbg = ReentrantCallbackGroup()
        srv_ser_write_cbg = MutuallyExclusiveCallbackGroup()
        normal_timer_cbg = MutuallyExclusiveCallbackGroup()
        read_timer_cbg = MutuallyExclusiveCallbackGroup()

        # Publishers
        self.status_pub = self.create_publisher(Bool, 'plc_connection', 10)
        self.releasing_mtrl_box_pub = self.create_publisher(Bool, 'releasing_material_box', 10)
        self.sliding_platform_curr_pub = self.create_publisher(UInt8MultiArray, 'sliding_platform_curr', 10)
        self.sliding_platform_cmd_pub = self.create_publisher(UInt8MultiArray, 'sliding_platform_cmd', 10)
        self.sliding_platform_ready_pub = self.create_publisher(UInt8MultiArray, 'sliding_platform_ready', 10)
        self.elevator_pub = self.create_publisher(Bool, 'elevator', 10)
        self.vision_block_pub = self.create_publisher(Bool, 'vision_block', 10)
        self.con_mtrl_box_pub = self.create_publisher(UInt8, 'container_material_box', 10)
        self.loc_sensor_pub = self.create_publisher(UInt8MultiArray, 'location_sensor', 10)

        # Service Servers
        self.read_srv = self.create_service(
            ReadRegister,
            "read_register",
            self.read_registers_cb,
            callback_group=srv_ser_read_cbg
        )
        self.write_srv = self.create_service(
            WriteRegister,
            "write_register",
            self.write_registers_cb,
            callback_group=srv_ser_write_cbg
        )

        # Timers
        self.connection_timer = self.create_timer(1.0, self.connection_cb, callback_group=normal_timer_cbg)
        self.status_timer = self.create_timer(1.0, self.status_cb, callback_group=normal_timer_cbg)
        self.releasing_mtrl_box_timer = self.create_timer(1.0, self.releasing_mtrl_box_cb, callback_group=read_timer_cbg)
        self.sliding_platform_curr_timer = self.create_timer(0.5, self.sliding_platform_curr_cb, callback_group=read_timer_cbg)
        self.sliding_platform_cmd_timer = self.create_timer(0.5, self.sliding_platform_cmd_cb, callback_group=read_timer_cbg)
        self.sliding_platform_ready_timer = self.create_timer(0.5, self.sliding_platform_ready_cb, callback_group=read_timer_cbg)
        self.elevator_timer = self.create_timer(0.5, self.elevator_cb, callback_group=read_timer_cbg)
        self.vision_block_timer = self.create_timer(0.25, self.vision_block_cb, callback_group=read_timer_cbg)
        self.con_mtrl_box_timer = self.create_timer(1.0, self.con_mtrl_box_cb, callback_group=read_timer_cbg)
        self.loc_sensor_timer = self.create_timer(0.5, self.loc_senser_cb, callback_group=read_timer_cbg)

        self.get_logger().info("PLC Modbus TCP Client Node is initialized successfully")

    def run_loop(self):
        """Run the asyncio event loop in a dedicated thread."""
        asyncio.set_event_loop(self.loop)
        self.get_logger().info("Started the event loop.")
        self.loop.run_forever()

    async def create_and_connect_client(self):
        with self.mutex:
            self.cli = Client(host=self.ip, port=self.port)
            self.get_logger().info("Created an AsyncModbusTcpClient Successfully")
            await self.cli.connect()
        if self.cli is not None and self.cli.connected:
            self.get_logger().info("Connected to Modbus server Successfully")
        else:
            self.get_logger().error("Failed to connect to Modbus server")

    # Timer Callbacks
    async def connection_cb(self) -> None:
        if not self.cli or not self.cli.connected:
            self.get_logger().info(f"Try to connect to {self.ip}:{self.port}")
            future = run_coroutine_threadsafe(self.create_and_connect_client(), self.loop)
            try:
                self.get_logger().info(f"Waiting the connection result {self.ip}:{self.port}")
                future.result()
            except Exception as e:
                self.get_logger().error(f"Failed to initialize Modbus client: {e}")
        else:
            self.get_logger().debug(f"Already connected to {self.ip}:{self.port}")

    def status_cb(self) -> None:
        is_connected = bool(self.cli and self.cli.connected)
        msg = Bool()
        msg.data = is_connected
        self.status_pub.publish(msg)
        self.get_logger().debug(f"Connected: {is_connected}")

    def releasing_mtrl_box_cb(self) -> None:
        if not self.cli or not self.cli.connected:
            return

        try:
            result = self._execute_read_action(ReqTemp.RELEASING_MTRL_BOX_REQ)

            data = result.registers
            self.releasing_mtrl_box_pub.publish(Bool(data=data[0] == 1))

            self.get_logger().debug(f"[Releasing State]\tRead Registers, "
                                    f"address: {ReqTemp.RELEASING_MTRL_BOX_REQ.address}, "
                                    f"count: {ReqTemp.RELEASING_MTRL_BOX_REQ.count}, values: [{', '.join(map(str, data))}]")
        except Exception as e:
            self.get_logger().error(f"Exception: {e}")

    def sliding_platform_curr_cb(self) -> None:
        if not self.cli or not self.cli.connected:
            return

        try:
            result = self._execute_read_action(ReqTemp.SLIDING_PLATFORM_CURR_REQ)

            data=result.registers
            self.sliding_platform_curr_pub.publish(UInt8MultiArray(data=data))

            self.get_logger().debug(f"[Current Sliding Platform]\tRead Registers, "
                                    f"address: {ReqTemp.SLIDING_PLATFORM_CURR_REQ.address}, "
                                    f"count: {ReqTemp.SLIDING_PLATFORM_CURR_REQ.count}, values: [{', '.join(map(str, data))}]")
        except Exception as e:
            self.get_logger().error(f"Exception: {e}")

    def sliding_platform_cmd_cb(self) -> None:
        if not self.cli or not self.cli.connected:
            return

        try:
            result = self._execute_read_action(ReqTemp.SLIDING_PLATFORM_CMD_REQ)
            
            data = result.registers
            self.sliding_platform_cmd_pub.publish(UInt8MultiArray(data=data))

            self.get_logger().debug(f"[Sliding Platform Movemnet]\tRead Registers, "
                                    f"address: {ReqTemp.SLIDING_PLATFORM_CMD_REQ.address}, "
                                    f"count: {ReqTemp.SLIDING_PLATFORM_CMD_REQ.count}, values: [{', '.join(map(str, data))}]")
        except Exception as e:
            self.get_logger().error(f"Exception: {e}")

    def sliding_platform_ready_cb(self) -> None:
        if not self.cli or not self.cli.connected:
            return

        try:
            result = self._execute_read_action(ReqTemp.SLIDING_PLATFORM_READY_REQ)

            data = result.registers
            self.sliding_platform_ready_pub.publish(UInt8MultiArray(data=data))

            self.get_logger().debug(f"[Sliding Platform Ready]\tRead Registers, "
                                    f"address: {ReqTemp.SLIDING_PLATFORM_READY_REQ.address}, "
                                    f"count: {ReqTemp.SLIDING_PLATFORM_READY_REQ.count}, values: [{', '.join(map(str, data))}]")
        except Exception as e:
            self.get_logger().error(f"Exception: {e}")

    def elevator_cb(self) -> None:
        if not self.cli or not self.cli.connected:
            return

        try:
            result = self._execute_read_action(ReqTemp.ELEVATOR_REQ)

            data = result.registers
            self.elevator_pub.publish(Bool(data=data[0] == 1))

            self.get_logger().debug(f"[Elevator]\t\t\tRead Registers, "
                                    f"address: {ReqTemp.ELEVATOR_REQ.address}, "
                                    f"count: {ReqTemp.ELEVATOR_REQ.count}, values: [{', '.join(map(str, data))}]")
        except Exception as e:
            self.get_logger().error(f"Exception: {e}")

    def vision_block_cb(self) -> None:
        if not self.cli or not self.cli.connected:
            return

        try:
            result = self._execute_read_action(ReqTemp.VISION_BLOCK_REQ)

            data = result.registers
            self.vision_block_pub.publish(Bool(data=data[0] == 1))

            self.get_logger().debug(f"[Elevator]\t\t\tRead Registers, "
                                    f"address: {ReqTemp.VISION_BLOCK_REQ.address}, "
                                    f"count: {ReqTemp.VISION_BLOCK_REQ.count}, values: [{', '.join(map(str, data))}]")
        except Exception as e:
            self.get_logger().error(f"Exception: {e}")

    def con_mtrl_box_cb(self) -> None:
        if not self.cli or not self.cli.connected:
            return

        try:
            result = self._execute_read_action(ReqTemp.CONTAINER_AMOUNT_REQ)

            data = result.registers
            self.con_mtrl_box_pub.publish(UInt8(data=data[0]))

            self.get_logger().debug(f"[Conatiner Material Box]\tRead Registers, "
                                    f"address: {ReqTemp.CONTAINER_AMOUNT_REQ.address}, "
                                    f"count: {ReqTemp.CONTAINER_AMOUNT_REQ.count}, values: [{', '.join(map(str, data))}]")
        except Exception as e:
            self.get_logger().error(f"Exception: {e}")

    def loc_senser_cb(self) -> None:
        if not self.cli or not self.cli.connected:
            return
        try:
            result = self._execute_read_action(ReqTemp.LOCATION_SENSOR_REQ)

            data = result.registers
            self.loc_sensor_pub.publish(UInt8MultiArray(data=data))

            self.get_logger().debug(f"[Conatiner Material Box]\tRead Registers, "
                                    f"address: {ReqTemp.LOCATION_SENSOR_REQ.address}, "
                                    f"count: {ReqTemp.LOCATION_SENSOR_REQ.count}, values: [{', '.join(map(str, data))}]")
        except Exception as e:
            self.get_logger().error(f"Exception: {e}")
        
    def _execute_read_action(self, req: ReadRegister.Request) -> Optional[ReadHoldingRegistersResponse]:
        """Execute a register write operation and return success status."""
        future = run_coroutine_threadsafe(self.async_read_registers(req), self.loop)
        try:
            result = future.result()

            if not isinstance(result, ReadHoldingRegistersResponse):
                raise Exception("result is not ReadHoldingRegistersResponse")
            if result.isError():
                raise Exception("result is Error")
            
            return result
        except Exception as e:
            self.get_logger().error(f"Movement execution failed: {str(e)}")
            return None

    async def read_registers_cb(self, req, res):
        if not self.cli or not self.cli.connected:
            return

        future = run_coroutine_threadsafe(
            self.async_read_registers(req), 
            self.loop
        )
        self.get_logger().info(f"Received a read register request: {req.address}")
        try:
            result = future.result()

            if not isinstance(result, ReadHoldingRegistersResponse):
                raise Exception("result is not ReadHoldingRegistersResponse")
            if result.isError():
                raise Exception("result is Error")

            data = result.registers
            res.success = True
            res.values = result.registers
        except Exception as e:
            self.get_logger().error(f"Exception: {e}")
            res.message = f"Exception: {e}"
        finally:
            return res

    async def write_registers_cb(self, req, res):
        if not self.cli or not self.cli.connected:
            return

        future = run_coroutine_threadsafe(
            self.async_write_registers(req), 
            self.loop
        )
        self.get_logger().info(f"Received a write register request: {req.address}, values: {req.values}")
        try:
            result = future.result()

            if not isinstance(result, WriteMultipleRegistersResponse):
                raise Exception("result is not WriteMultipleRegistersResponse")
            if result.isError():
                raise Exception("result is Error")

            res.success = True
        except Exception as e:
            self.get_logger().error(f"Exception: {e}")
            res.message = f"Exception: {e}"
        finally:
            return res

    async def async_read_registers(self, req):
        """Handle read_registers service reqs."""
        if not self.cli or not self.cli.connected:
            raise Exception("Modbus client is not connected.")

        return await self.cli.read_holding_registers(
            address=req.address,
            count=req.count,
            slave=1
        )

    async def async_write_registers(self, req):
        """Handle write_registers service reqs."""
        if not self.cli or not self.cli.connected:
            raise Exception("Modbus client is not connected.")

        return await self.cli.write_registers(
            address=req.address,
            values=req.values,
            slave=1
        )

    def destroy_node(self):
        """Clean up resources when the node is destroyed."""
        if self.cli is not None or self.cli.connected:
            self.cli.close()
            self.get_logger().info("Closing Modbus connection...")

        if self.loop.is_running():
            self.loop.call_soon_threadsafe(self.loop.stop)
            self.get_logger().info("Removed loop successfully")

        if self.loop_thread is not None:
            self.loop_thread.join()
            self.get_logger().info("Removed loop thread successfully")

        super().destroy_node()


async def async_main(args=None):
    rclpy.init(args=args)
    try:
        node = PlcComm()
        executor = MultiThreadedExecutor()
        executor.add_node(node)
        try:
            executor.spin()
        finally:
            executor.shutdown()
            node.destroy_node()
    except KeyboardInterrupt:
        pass
    except ExternalShutdownException:
        sys.exit(1)
    finally:
        rclpy.try_shutdown()


def main():
    asyncio.run(async_main())


if __name__ == '__main__':
    main()