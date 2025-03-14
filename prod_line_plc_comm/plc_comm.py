import asyncio
from threading import Thread, Lock
import rclpy

from rclpy.node import Node
from rclpy.executors import ExternalShutdownException, SingleThreadedExecutor

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

        self.cli = None
        self.loop = asyncio.new_event_loop()
        self.loop_thread = Thread(target=self.run_loop, daemon=True)
        self.loop_thread.start()
        self.mutex = Lock()

        self.connection_timer = self.create_timer(0.2, self.connection_cb)
        self.status_timer = self.create_timer(1.0, self.status_cb)
        self.releasing_mtrl_box_timer = self.create_timer(0.2, self.releasing_mtrl_box_cb)
        self.sliding_platform_timer = self.create_timer(0.2, self.sliding_platform_cb)
        self.sliding_platform_movement_timer = self.create_timer(0.2, self.sliding_platform_movement_cb)

        self.status_pub = self.create_publisher(Bool, 'plc_connection', 10)
        self.releasing_mtrl_box_pub = self.create_publisher(Bool, 'releasing_material_box', 10)
        self.sliding_platform_pub = self.create_publisher(UInt8MultiArray, 'sliding_platform', 10)
        self.sliding_platform_movement_pub = self.create_publisher(UInt8MultiArray, 'sliding_platform_movement', 10)

        self.read_srv = self.create_service(
            ReadRegister,
            "read_register",
            self.read_registers_cb,
        )
        self.write_srv = self.create_service(
            WriteRegister,
            "write_register",
            self.write_registers_cb,
        )

        self.get_logger().info("PLC ModbusTcpClient Node initialized")

    def run_loop(self):
        """Run the asyncio event loop in a dedicated thread."""
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()
        self.get_logger().info("Started the event loop.")

    async def create_and_connect_client(self):
        self.cli = Client(host=self.ip, port=self.port)
        await self.cli.connect()
        if self.cli is not None and self.cli.connected:
            self.get_logger().info("Connected to Modbus server Successfully")
        else:
            self.get_logger().error("Failed to connect to Modbus server")

    def status_cb(self):
        is_connected = bool(self.cli and self.cli.connected)
        msg = Bool()
        msg.data = is_connected
        self.status_pub.publish(msg)
        self.get_logger().debug(f"Connected: {is_connected}")

    async def releasing_mtrl_box_cb(self):
        req = ReadRegister.Request()
        req.address = 5500
        req.count = 1
        future = asyncio.run_coroutine_threadsafe(self.async_read_registers(req), self.loop)
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
            self.get_logger().debug(f"Read Registers, address: {req.address}, count: {req.count}, values: [{', '.join(map(str, data))}]")
        except Exception as e:
            self.get_logger().error(f"Exception: {e}")

    async def sliding_platform_cb(self):
        req = ReadRegister.Request()
        req.address = 5101
        req.count = 14
        future = asyncio.run_coroutine_threadsafe(self.async_read_registers(req), self.loop)
        try:
            result = future.result()
            if not isinstance(result, ReadHoldingRegistersResponse):
                raise Exception("result is not ReadHoldingRegistersResponse")
            if result.isError():
                raise Exception("result is Error")
            data = result.registers
            msg = UInt8MultiArray()
            msg.data = data
            self.sliding_platform_pub.publish(msg)
            self.get_logger().debug(f"Read Registers, address: {req.address}, count: {req.count}, values: [{', '.join(map(str, data))}]")
        except Exception as e:
            self.get_logger().error(f"Exception: {e}")

    async def sliding_platform_movement_cb(self):
        req = ReadRegister.Request()
        req.address = 5001
        req.count = 14
        future = asyncio.run_coroutine_threadsafe(self.async_read_registers(req), self.loop)
        try:
            result = future.result()
            if not isinstance(result, ReadHoldingRegistersResponse):
                raise Exception("result is not ReadHoldingRegistersResponse")
            if result.isError():
                raise Exception("result is Error")
            data = result.registers
            msg = UInt8MultiArray()
            msg.data = data
            self.sliding_platform_movement_pub.publish(msg)
            self.get_logger().debug(f"Read Registers, address: {req.address}, count: {req.count}, values: [{', '.join(map(str, data))}]")
        except Exception as e:
            self.get_logger().error(f"Exception: {e}")

    def connection_cb(self):
        with self.mutex:
            if self.cli is None or not self.cli.connected:
                self.get_logger().info(f"Try to connect to {self.ip}:{self.port}")
                future = asyncio.run_coroutine_threadsafe(self.create_and_connect_client(), self.loop)
                try:
                    future.result()
                except Exception as e:
                    self.get_logger().error(f"Failed to initialize Modbus client: {e}")
            else:
                self.get_logger().debug(f"Already connected to {self.ip}:{self.port}")

    async def read_registers_cb(self, req, res):
        future = asyncio.run_coroutine_threadsafe(self.async_read_registers(req), self.loop)
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
        """Handle read_registers service reqs."""
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
            self.get_logger().info("Closing Modbus connection...")
            self.cli.close()
        if self.loop.is_running():
            self.get_logger().info("Remove loop successfully")
            self.loop.call_soon_threadsafe(self.loop.stop)
        if self.loop_thread is not None:
            self.get_logger().info("Remove loop thread successfully")
            self.loop_thread.join()
        super().destroy_node()


async def async_main(args=None):
    try:
        rclpy.init(args=args)
        node = PlcComm()
        executor = SingleThreadedExecutor()
        executor.add_node(node)
        executor.spin()
    except (KeyboardInterrupt, ExternalShutdownException):
        pass
    finally:
        if rclpy.ok():
            rclpy.shutdown()


def main():
    asyncio.run(async_main())


if __name__ == '__main__':
    main()