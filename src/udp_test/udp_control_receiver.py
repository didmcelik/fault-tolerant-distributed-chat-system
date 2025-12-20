import socket

CONTROL_PORT = 37021

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind(("0.0.0.0", CONTROL_PORT))

print(f"[RECEIVER] Listening on UDP {CONTROL_PORT}")

while True:
    data, addr = sock.recvfrom(4096)
    print(f"[RECEIVER] Got from {addr}: {data.decode()}")
