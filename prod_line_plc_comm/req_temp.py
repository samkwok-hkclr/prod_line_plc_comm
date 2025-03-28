from typing import final, Final

from smdps_msgs.srv import ReadRegister

@final
class RequestTemplate:
    RELEASING_MTRL_BOX_REQ: Final[ReadRegister.Request] = ReadRegister.Request(
        address = 5500,
        count = 1
    )

    SLIDING_PLATFORM_CURR_REQ: Final[ReadRegister.Request] = ReadRegister.Request(
        address = 5101,
        count = 14   
    )

    SLIDING_PLATFORM_CMD_REQ: Final[ReadRegister.Request] = ReadRegister.Request(
        address = 5001,
        count = 14
    )

    SLIDING_PLATFORM_READY_REQ: Final[ReadRegister.Request] = ReadRegister.Request(
        address = 5301,
        count = 14
    )

    ELEVATOR_REQ: Final[ReadRegister.Request] = ReadRegister.Request(
        address = 5030,
        count = 1
    )

    VISION_BLOCK_REQ: Final[ReadRegister.Request] = ReadRegister.Request(
        address = 5400,
        count = 1  
    )
    
    CONTAINER_AMOUNT_REQ: Final[ReadRegister.Request] = ReadRegister.Request(
        address = 5410,
        count = 1  
    )
   
    LOCATION_SENSOR_REQ: Final[ReadRegister.Request] = ReadRegister.Request(
        address = 7900,
        count = 77
    )
    