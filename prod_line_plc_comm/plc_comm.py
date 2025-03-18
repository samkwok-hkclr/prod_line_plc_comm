import sys
import asyncio
from threading import Thread, Lock
import rclpy

from rclpy.node import Node
from rclpy.callback_groups import MutuallyExclusiveCallbackGroup
from rclpy.executors import ExternalShutdownException, MultiThreadedExecutor

from pymodbus.client import AsyncModbusTcpClient as Client
from pymodbus.pdu.register_message import ReadHoldingRegistersResponse, WriteMultipleRegistersResponse

from std_msgs.msg import Bool, UInt8MultiArray
from smdps_msgs.srv import ReadRegister, WriteRegister

class PlcComm(Node):
    def __init__(self):
        super().__init__("plc_comm")
        self.declare_parameter("ip", "127.0.0.1")
        self.declare_parameter("port", 502)

        self.ip = self.get_parameter("ip").value
        self.port = self.get_parameter("port").value

        # Requests
        self.releasing_mtrl_box_req = ReadRegister.Request()
        self.releasing_mtrl_box_req.address = 5500
        self.releasing_mtrl_box_req.count = 1
        self.sliding_platform_curr_req = ReadRegister.Request()
        self.sliding_platform_curr_req.address = 5101
        self.sliding_platform_curr_req.count = 14
        self.sliding_platform_cmd_req = ReadRegister.Request()
        self.sliding_platform_cmd_req.address = 5001
        self.sliding_platform_cmd_req.count = 14
        self.sliding_platform_ready_req = ReadRegister.Request()
        self.sliding_platform_ready_req.address = 5301
        self.sliding_platform_ready_req.count = 14
        self.elevator_req = ReadRegister.Request()
        self.elevator_req.address = 5030
        self.elevator_req.count = 1

        # Modbus TCP Client
        self.cli = None

        self.loop = asyncio.new_event_loop()
        self.loop_thread = Thread(target=self.run_loop, daemon=True)
        self.loop_thread.start()
        self.mutex = Lock()

        # Callback groups
        srv_ser_read_cbg = MutuallyExclusiveCallbackGroup()
        srv_ser_write_cbg = MutuallyExclusiveCallbackGroup()
        normal_timer_cbg = MutuallyExclusiveCallbackGroup()
        read_timer_cbg = MutuallyExclusiveCallbackGroup()

        # Publishers
        self.status_pub = self.create_publisher(Bool, 'plc_connection', 10)
        self.releasing_mtrl_box_pub = self.create_publisher(Bool, 'releasing_material_box', 10)
        self.sliding_platform_curr_pub = self.create_publisher(UInt8MultiArray, 'sliding_platform_current', 10)
        self.sliding_platform_cmd_pub = self.create_publisher(UInt8MultiArray, 'sliding_platform_cmd', 10)
        self.sliding_platform_ready_pub = self.create_publisher(UInt8MultiArray, 'sliding_platform_ready', 10)
        self.elevator_pub = self.create_publisher(Bool, 'elevator', 10)

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
        self.connection_timer = self.create_timer(0.1, self.connection_cb, callback_group=normal_timer_cbg)
        self.status_timer = self.create_timer(1.0, self.status_cb, callback_group=normal_timer_cbg)
        self.releasing_mtrl_box_timer = self.create_timer(0.2, self.releasing_mtrl_box_cb, callback_group=read_timer_cbg)
        self.sliding_platform_curr_timer = self.create_timer(0.5, self.sliding_platform_curr_cb, callback_group=read_timer_cbg)
        self.sliding_platform_cmd_timer = self.create_timer(1.0, self.sliding_platform_cmd_cb, callback_group=read_timer_cbg)
        self.sliding_platform_ready_timer = self.create_timer(0.125, self.sliding_platform_ready_cb, callback_group=read_timer_cbg)
        self.elevator_timer = self.create_timer(1.0, self.elevator_cb, callback_group=read_timer_cbg)

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
    async def connection_cb(self):
        if self.cli is None or not self.cli.connected:
            self.get_logger().info(f"Try to connect to {self.ip}:{self.port}")
            future = asyncio.run_coroutine_threadsafe(self.create_and_connect_client(), self.loop)
            try:
                self.get_logger().info(f"Waiting the connection result {self.ip}:{self.port}")
                future.result()
            except Exception as e:
                self.get_logger().error(f"Failed to initialize Modbus client: {e}")
        else:
            self.get_logger().debug(f"Already connected to {self.ip}:{self.port}")

    def status_cb(self):
        is_connected = bool(self.cli and self.cli.connected)
        msg = Bool()
        msg.data = is_connected
        self.status_pub.publish(msg)
        self.get_logger().debug(f"Connected: {is_connected}")

    async def releasing_mtrl_box_cb(self):
        future = asyncio.run_coroutine_threadsafe(self.async_read_registers(self.releasing_mtrl_box_req), self.loop)
        try:
            result = future.result()
            if not isinstance(result, ReadHoldingRegistersResponse):
                raise Exception("result is not ReadHoldingRegistersResponse")
            if result.isError():
                raise Exception("result is Error")
            data = result.registers
            msg = Bool()
            msg.data = bool(data[0] == 1)
            self.releasing_mtrl_box_pub.publish(msg)
            self.get_logger().info(f"[Releasing State]\tRead Registers, address: {self.releasing_mtrl_box_req.address}, count: {self.releasing_mtrl_box_req.count}, values: [{', '.join(map(str, data))}]")
        except Exception as e:
            self.get_logger().error(f"Exception: {e}")

    async def sliding_platform_curr_cb(self):
        future = asyncio.run_coroutine_threadsafe(self.async_read_registers(self.sliding_platform_curr_req), self.loop)
        try:
            result = future.result()
            if not isinstance(result, ReadHoldingRegistersResponse):
                raise Exception("result is not ReadHoldingRegistersResponse")
            if result.isError():
                raise Exception("result is Error")
            data = result.registers
            msg = UInt8MultiArray()
            msg.data = data
            self.sliding_platform_curr_pub.publish(msg)
            self.get_logger().debug(f"[Current Sliding Platform]\tRead Registers, address: {self.sliding_platform_curr_req.address}, count: {self.sliding_platform_curr_req.count}, values: [{', '.join(map(str, data))}]")
        except Exception as e:
            self.get_logger().error(f"Exception: {e}")

    async def sliding_platform_cmd_cb(self):
        future = asyncio.run_coroutine_threadsafe(self.async_read_registers(self.sliding_platform_cmd_req), self.loop)
        try:
            result = future.result()
            if not isinstance(result, ReadHoldingRegistersResponse):
                raise Exception("result is not ReadHoldingRegistersResponse")
            if result.isError():
                raise Exception("result is Error")
            data = result.registers
            msg = UInt8MultiArray()
            msg.data = data
            self.sliding_platform_cmd_pub.publish(msg)
            self.get_logger().debug(f"[Sliding Platform Movemnet]\tRead Registers, address: {self.sliding_platform_cmd_req.address}, count: {self.sliding_platform_cmd_req.count}, values: [{', '.join(map(str, data))}]")
        except Exception as e:
            self.get_logger().error(f"Exception: {e}")

    async def sliding_platform_ready_cb(self):
        future = asyncio.run_coroutine_threadsafe(self.async_read_registers(self.sliding_platform_ready_req), self.loop)
        try:
            result = future.result()
            if not isinstance(result, ReadHoldingRegistersResponse):
                raise Exception("result is not ReadHoldingRegistersResponse")
            if result.isError():
                raise Exception("result is Error")
            data = result.registers
            msg = UInt8MultiArray()
            msg.data = data
            self.sliding_platform_ready_pub.publish(msg)
            self.get_logger().debug(f"[Sliding Platform Ready]\tRead Registers, address: {self.sliding_platform_ready_req.address}, count: {self.sliding_platform_ready_req.count}, values: [{', '.join(map(str, data))}]")
        except Exception as e:
            self.get_logger().error(f"Exception: {e}")

    async def elevator_cb(self):
        future = asyncio.run_coroutine_threadsafe(self.async_read_registers(self.elevator_req), self.loop)
        try:
            result = future.result()
            if not isinstance(result, ReadHoldingRegistersResponse):
                raise Exception("result is not ReadHoldingRegistersResponse")
            if result.isError():
                raise Exception("result is Error")
            data = result.registers
            msg = Bool()
            msg.data = bool(data[0] == 1)
            self.elevator_pub.publish(msg)
            self.get_logger().debug(f"[Elevator]\t\t\tRead Registers, address: {self.elevator_req.address}, count: {self.elevator_req.count}, values: [{', '.join(map(str, data))}]")
        except Exception as e:
            self.get_logger().error(f"Exception: {e}")

    async def read_registers_cb(self, req, res):
        future = asyncio.run_coroutine_threadsafe(self.async_read_registers(req), self.loop)
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
        future = asyncio.run_coroutine_threadsafe(self.async_write_registers(req), self.loop)
        self.get_logger().info(f"Received a write register request: {req.address}")
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