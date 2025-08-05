DATA_NEOSMARTBLINDS = "neosmartblinds"

CONF_DEVICE = "blind_code"
CONF_CLOSE_TIME = "close_time"
CONF_ID = "hub_id"
CONF_PROTOCOL = "protocol"
CONF_PORT = "port"
CONF_RAIL = "rail"
CONF_PERCENT_SUPPORT = "percent_support"
CONF_MOTOR_CODE = "motor_code"
CONF_START_POSITION = "start_position"
CONF_PARENT = "parent_group"

LEGACY_POSITIONING = 0
EXPLICIT_POSITIONING = 1
IMPLICIT_POSITIONING = 2

ACTION_STOPPED = 0
ACTION_OPENING = 1
ACTION_CLOSING = 2

DEFAULT_IO_TIMEOUT = 10
DEFAULT_COMMAND_AGGREGATION_PERIOD = 0.25
DEFAULT_COMMAND_BACKOFF = 0.7

CMD_UP = "up"
CMD_DOWN = "dn"
CMD_MICRO_UP = "mu"
CMD_MICRO_DOWN = "md"
CMD_STOP = "sp"
CMD_FAV = "gp"
CMD_FAV_1 = "i1"
CMD_FAV_2 = "i2"

# Commands below are not available through home assistant as of yet.
CMD_SET_FAV = "pp"
CMD_REVERSE = "rv"
CMD_CONFIRM = "sc"
CMD_LIMIT = "ld"

# Below are use for Top Down / Bottom Up blinds
# used for "rail" 2 which is the top of the blind.
CMD_UP2 = "u2"
CMD_DOWN2 = "d2"
CMD_MICRO_UP2 = "o2"
CMD_MICRO_DOWN2 = "c2"

# Used for "rail" 3 which is both top and bottom moving at once
# API doc says it should move both rails up or down
CMD_UP3 = "u3"
CMD_DOWN3 = "d3"

# commands for fully opening and closing a top-down/bottom-up blind
CMD_TDBU_OPEN = "op"
CMD_TDBU_CLOSE = "cl"
