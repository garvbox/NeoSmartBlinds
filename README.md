# Custom Component for NeoSmartBlinds Integration on Home Assistant

The NeoSmartBlinds platform allows you to control a NeoSmartBlind / group of NeoSmartBlinds via a NeoSmartBlinds controller.

There is currently support for the following device types within Home Assistant:

-   Cover

## Installation

To begin with it is recommended you ensure your NeoSmartBlinds controller has a static IP addresses, you may need to configure this via your router's DHCP options.

1. Download the latest release from: https://github.com/garbox/NeoSmartBlinds/releases
2. Extract the file
3. Copy the whole **neosmartblinds** folder from **custom_components/** to your own **config/custom_components/** directory
4. Go to Config -> Server Controls -> Under "Server Management" click restart
5. Go to configuration example for how to setup your config.

Alternatively, you can install through [HACS](https://hacs.xyz) by adding this repository.


# Cover Configuration 

### Example of basic configuration.yaml
```
cover:
  - platform: neosmartblinds
    name: Blind One
    host: 192.168.0.13
    hub_id: 000000000000000000000000
    protocol: http
    port: 8838
    blind_code: 021.230-04
    close_time: 30
    rail: 1
    percent_support: 1
    motor_code: bf
    start_position: 50
    parent_group: 021.230-15
```

## Configuration variables

cover:

**platform** (String)(Required) <br>
Must be set to neosmartblinds

**host** _(String)(Required)_<br>
The IP of the NeoSmartBlinds controller, e.g., 192.168.0.10.

Getting the IP:

![Getting the IP](doc_images/app_controller_ip.jpg)



**hub_id** _(String)(Required)_<br>
The 24 character device ID of the Smart Blinds Hub, found in the APP

![App Main Screen](doc_images/app_main_window.jpg)
![App Menu](doc_images/app_menu.jpg)
![App Controllers](doc_images/app_controllers.jpg)

**protocol** _(String)(Required)_<br>
The protocol to use for sending commands. (http, tcp)

**port** _(String)(Required)_<br>
Port use for the connection.  for TCP use 8839, for HTTP use 8838

**name** _(String)(Required)_<br>
The name you would like to give to the NeoSmartBlind.

**blind_code** _(String)(Required)_<br>
The blind code. - This is available from the NeoSmartBlind app<br>
Getting the codes: 

![Blind Code](doc_images/app_blind_codes.jpg)

- Room Code: will control all blinds in that room
- Blind Code: will controll only that blind

**“​ID1.ID2-CHANNEL”**<br>
**“ID1”** : controller byte 1, from integer “000” to “255”<br>
**“.”** : address separator<br>
**“ID2”** : controller byte 2, from integer “000” to “255”<br>
**“-”** : channel separator<br>
**“CHANNEL”** : channel, individual channel from integer “01” to “14”, use channel “15” for a group

**close_time** _(String)(Required)_<br>
Time taken in seconds to close this blind (use a stop watch to measure)

**rail** _(string)_<br>
Rail Number used to determine top or bottom rail on top down/bottom up blinds. <br>
1 = Top Rail<br>
2 = Bottom Rail

**percent_support** _(int)_<br>
Determines how requests to position by percentage are handled. As the blinds do not report back their position, this integration can only estimate the position based on the close_time option <br>
0 = No support (Default)<br>
1 = Percentage positioning supported directly by blind, integration only reports position based on estimate<br>
2 = Percentage positioning emulated completely by this integration.

Notes:
- If the blind is controlled outside of HA (e.g. in native app, using remote), this integration has no mechanism to discover the position from the hub so HA will get out of sync with the blind.
- For the same reason, the integration will initialise on startup according to start_position. This is a guess. Issuing an open or close command to the blind (through this integration) will allow the positions to sync. Depending on setup, an automation that listens for HA to start could be used to either open or close the blinds to get them into sync.
- If exposing the blind via HomeKit, either option 1 or 2 should be selected to keep the actual position and Home in sync. 

**motor_code** _(string)_<br>
Defines the motor code listed in the neo smart blinds app on your phone.  Listed below the 'Blind Code' on the control page for the blind <br>
This is required for some smart hubs to work (model C-BR300) 

**start_position** _(int)_<br>
Optional starting position for the blind when HA starts up. If not specified, the integration will restore the position saved from the last time HA was shutdown. This value is ignored if percent_support is zero.

**parent_group** _(String)_<br>
See blind_code for instructions on how to find the room code. This parameter is optional. <br>
If the same group is specified for multiple blinds, the integration will pause briefly (250ms) before issuing a command to see whether to 
send to the individual blind or to the parent group. As the hub is limited in the number of requests it can handle, this can help in cases
where multiple blinds are controlled in home assistant or via Homekit / Alexa etc. 

<br><br>


Entity Options in UI:

![Entity Options](doc_images/EntityOptions.JPG)

Entity Control allows for fine adjustment and extra controls:

![Entity Control](doc_images/EntityControl.JPG)

Lovelace ui panel provides basic control

![Lovelace UI panel](doc_images/Lovelace_UI_Panel.JPG)

## Supported features

**Open**
Up

**Close**
Down

**Tilt-Up**
Micro-Up

**Tilt-Down**
Micro-Down

**Set-Position & Favourite Position** - please note this is calculated using the close_time

   ### Setting the position if "percent_support" is 0:
   
   **==50** will set your blind to its stored first favourite position 

   **==51** will set your blind to its stored second favourite position 

   ### Setting the position if "percent_support" is 1:
   
   **Setting position** Use the position slider to select how, blind will move to that position
      
   **Setting favorite postion 1** Use the tilt slider to select a value less than 50
   
   **Setting favorite postion 2** Use the tilt slider to select a value greater than 50

   ### Setting the position if "percent_support" is 2:
   
   **Setting position** Use the position slider to select how, integration will move blind to position and then send stop command
      
   **Setting favorite postion 1** Use the tilt slider to select a value less than 50
   
   **Setting favorite postion 2** Use the tilt slider to select a value greater than 50

