# mqtt-forward

On-demand TCP and shell tunneling over MQTT v5. Two peers - client and device - communicate through an MQTT broker. The client initiates tunnel requests; the device accepts them and connects to local services or spawns shells. Gateway mode runs the client side for many devices at once, forwarding multiple local listeners to multiple devices over a single connection - useful behind a reverse proxy. Access control is delegated entirely to MQTT broker topic ACLs.
