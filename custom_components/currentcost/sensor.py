"""Support for reading Current Cost data from a serial port."""
import logging
import sys
import xmltodict
import serial_asyncio
import voluptuous as vol

from homeassistant.components.sensor import (
    PLATFORM_SCHEMA,
    SensorDeviceClass,
    SensorEntity,
    SensorStateClass,
)
from homeassistant.const import (
    CONF_DEVICES,
    CONF_NAME,
    CONF_UNIQUE_ID,
    EVENT_HOMEASSISTANT_STOP,
    STATE_UNAVAILABLE,
    STATE_UNKNOWN,
    UnitOfPower,
)
import homeassistant.helpers.config_validation as cv
from homeassistant.helpers.restore_state import RestoreEntity

_LOGGER = logging.getLogger(__name__)

CONF_SERIAL_PORT = "serial_port"
CONF_BAUDRATE = "baudrate"

DEFAULT_NAME = "Current Cost"
DEFAULT_ID = "abaaa250-fd59-46e1-abd8-07545fb2b297"
DEFAULT_BAUDRATE = 57600
DEFAULT_DEVICES = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]

PLATFORM_SCHEMA = PLATFORM_SCHEMA.extend(
    {
        vol.Required(CONF_SERIAL_PORT): cv.string,
        vol.Optional(CONF_BAUDRATE, default=DEFAULT_BAUDRATE): cv.positive_int,
        vol.Optional(CONF_NAME, default=DEFAULT_NAME): cv.string,
        vol.Optional(CONF_UNIQUE_ID, default=DEFAULT_ID): cv.string,
        vol.Optional(CONF_DEVICES, default=DEFAULT_DEVICES): vol.All(
            cv.ensure_list, [vol.Range(min=0, max=9)]
        ),
    }
)


async def async_setup_platform(hass, config, async_add_entities, discovery_info=None):
    """Set up the Current Cost sensor platform."""
    name = config.get(CONF_NAME)
    unique_id = config.get(CONF_UNIQUE_ID)
    port = config.get(CONF_SERIAL_PORT)
    baudrate = config.get(CONF_BAUDRATE)
    devices = config.get(CONF_DEVICES)

    sensor = CurrentCostSensor(name, f"current-cost-{unique_id}", port, baudrate, devices)

    # Correct shutdown listener registration without immediate execution
    async def _async_on_stop(event):
        await sensor.stop_serial_read()

    hass.bus.async_listen_once(EVENT_HOMEASSISTANT_STOP, _async_on_stop)
    async_add_entities([sensor])


class CurrentCostSensor(SensorEntity, RestoreEntity):
    """Representation of a Current Cost sensor."""

    def __init__(self, name, unique_id, port, baudrate, devices):
        """Initialize the Current Cost sensor."""
        self._name = name
        self._attr_unique_id = unique_id
        self._unit = UnitOfPower.WATT
        self._icon = "mdi:flash-outline"
        self._device_class = SensorDeviceClass.POWER
        self._state_class = SensorStateClass.MEASUREMENT
        self._state = None
        self._port = port
        self._baudrate = baudrate
        self._serial_loop_task = None
        self._serial_transport = None
        self._attributes = {"Temperature": None}
        self._devices = devices

        # Persist running totals on instance
        self._appliance1_total = 0
        self._appliance2_total = 0

        for variable in devices:
            self._attributes[f"Appliance {variable}"] = None
            self._attributes[f"Appliance {variable} Last 24h"] = None
            self._attributes[f"Appliance {variable} Last 30 days"] = None

    async def async_added_to_hass(self):
        """Handle entity restoration and startup."""
        await super().async_added_to_hass()
        last_state = await self.async_get_last_state()

        if last_state:
            # 1. Restore primary state
            if last_state.state not in (None, STATE_UNKNOWN, STATE_UNAVAILABLE):
                try:
                    self._state = float(last_state.state) if "." in last_state.state else int(last_state.state)
                except (ValueError, TypeError):
                    self._state = last_state.state

            # 2. Restore all cached attributes
            if last_state.attributes:
                for key, val in last_state.attributes.items():
                    if val not in (None, STATE_UNKNOWN, STATE_UNAVAILABLE):
                        self._attributes[key] = val
        else:
            _LOGGER.debug("No previous state available to restore")

        self._serial_loop_task = self.hass.loop.create_task(
            self.serial_read(self._port, self._baudrate)
        )

    async def serial_read(self, device, rate, **kwargs):
        """Read continuous data stream from serial port."""
        try:
            reader, writer = await serial_asyncio.open_serial_connection(
                url=device, baudrate=rate, **kwargs
            )
            self._serial_transport = writer.transport
        except Exception as error:
            _LOGGER.error("Failed to connect to serial port %s: %s", device, error)
            return

        while True:
            try:
                line = await reader.readline()
                line = line.decode("utf-8").strip()
                _LOGGER.debug("Line Received: %s", line)
            except Exception as error:
                _LOGGER.error("Error Reading From Serial Port: %s", error)
                continue

            try:
                data = xmltodict.parse(line)
                msg = data.get("msg", {})

                try:
                    appliance = int(msg.get("sensor"))
                except (TypeError, ValueError):
                    appliance = None

                # Parse temperature
                temperature = None
                if "tmpr" in msg:
                    try:
                        temperature = float(msg["tmpr"])
                    except (TypeError, ValueError):
                        pass
                elif "tmprF" in msg:
                    try:
                        temperature = float(msg["tmprF"])
                    except (TypeError, ValueError):
                        pass

                # Parse pulse inputs
                try:
                    imp = int(msg["imp"])
                    ipu = int(msg["ipu"])
                except (KeyError, TypeError, ValueError):
                    imp = None
                    ipu = None

                # Parse channels safely
                def _get_watts(ch_key):
                    try:
                        return int(msg.get(ch_key, {}).get("watts", 0))
                    except (TypeError, ValueError, AttributeError):
                        return 0

                watsch1 = _get_watts("ch1")
                watsch2 = _get_watts("ch2")
                watsch3 = _get_watts("ch3")
                total_watts = watsch1 + watsch2 + watsch3

                if appliance == 0:
                    self._attributes["Channel 1"] = watsch1
                    self._attributes["Channel 2"] = watsch2
                    self._attributes["Channel 3"] = watsch3

                if appliance is not None:
                    if imp is not None:
                        self._attributes[f"Impulses {appliance}"] = imp
                        self._attributes[f"Impulses/Unit {appliance}"] = ipu
                    else:
                        self._attributes[f"Appliance {appliance}"] = total_watts

                if temperature is not None:
                    self._attributes["Temperature"] = temperature

                # Update running instance totals
                if appliance == 1:
                    self._appliance1_total = total_watts
                elif appliance == 2:
                    self._appliance2_total = total_watts

                self._state = self._appliance1_total + self._appliance2_total

                # History extraction
                hist_data = msg.get("hist", {}).get("data", [])
                for variable in self._devices:
                    try:
                        item = hist_data[int(variable)] if isinstance(hist_data, list) else hist_data
                        if int(item.get("sensor")) == int(variable):
                            appliance_hist = int(item.get("sensor"))
                            if "d001" in item:
                                self._attributes[f"Appliance {appliance_hist} Last 24h"] = float(item["d001"])
                            if "m001" in item:
                                self._attributes[f"Appliance {appliance_hist} Last 30 days"] = float(item["m001"])
                    except Exception:
                        pass

                self.async_write_ha_state()

            except Exception:
                _LOGGER.error(
                    "Error parsing data from serial port:\n    %s\n    line received:\n    %s",
                    sys.exc_info()[1],
                    line,
                )

    async def stop_serial_read(self):
        """Close running background tasks and serial transport."""
        if self._serial_loop_task:
            self._serial_loop_task.cancel()
        if self._serial_transport:
            self._serial_transport.close()

    @property
    def name(self):
        """Return the name of the sensor."""
        return self._name

    @property
    def should_poll(self):
        """No polling needed."""
        return False

    @property
    def extra_state_attributes(self):
        """Return the attributes of the entity."""
        return self._attributes

    @property
    def state(self):
        """Return the state of the sensor."""
        return self._state

    @property
    def unit_of_measurement(self):
        """Return the units of measurement."""
        return self._unit

    @property
    def icon(self):
        """Return the icon of the sensor."""
        return self._icon

    @property
    def device_class(self):
        """Return the device class of the sensor."""
        return self._device_class

    @property
    def state_class(self):
        """Return the state class of the sensor."""
        return self._state_class
