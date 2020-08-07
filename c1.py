import rpyc
from rpyc import ThreadedServer
import sys

class Service1(rpyc.Service):
	
	def exposed_send_message(self,inp,port):
		c=rpyc.connect('localhost',port=port)
		c.root.receive_data(inp,port)
	def exposed_receive_data(self,message,port):
		print(message,port)
# inp=input('Enter the message\n')
# port=input('Enter the port you want to connect to')
# m=Service1()
# m.exposed_send_message(inp,port)
def print_data():
	inp=input('Enter the message')
	port=int(input('Enter the port you want to connect to'))

if __name__=='__main__':
	
	t=ThreadedServer(Service1,port=int(sys.argv[1]))
	t.start()
	
	
