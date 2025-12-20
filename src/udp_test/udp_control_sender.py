import socket
import time


#Didem ip 172.20.10.2
TARGET_IP = "172.20.10.3"   # receiver IP
CONTROL_PORT = 37021

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

i = 0
while True:
    msg = f"CONTROL_TEST {i}"
    sock.sendto(msg.encode(), (TARGET_IP, CONTROL_PORT))
    print(f"[SENDER] Sent: {msg}")
    i += 1
    time.sleep(1)
