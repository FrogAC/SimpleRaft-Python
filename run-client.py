
from raft.messages import ClientMsg, ClientMsgReply

import socket, pickle

seq = 0

def sendCommand(so, cmd):
	global seq
	port = so.getsockname()[1]
	msg = ClientMsg(port, seq, cmd)
	so.sendto(pickle.dumps(msg), ('localhost', 23333))
	data, addr = so.recvfrom(1024)
	reply = pickle.loads(data)
	print('Command', reply.seqnum, reply.success)
	seq += 1

def sendDebug(so):
	port = so.getsockname()[1]
	msg = ClientMsg(port, seq, 'debug')
	so.sendto(pickle.dumps(msg), ('localhost', 23333))

if __name__ == '__main__':
	so = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	so.bind(('localhost', 0))
	
	while True:
		cmd = input('next command: ')
		if cmd == 'debug':
			sendDebug(so)
		elif cmd == 'exit':
			break
		else:
			sendCommand(so, cmd)

	sendDebug(so)	
	

