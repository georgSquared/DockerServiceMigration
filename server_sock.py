import socket
import subprocess

s = socket.socket()

#host = "10.96.12.68"
host = ''
port = 8000
s.bind((host, port))
s.listen(5)


#Outer loop waits for connections and spawns clients
while True:

   print "Waiting for connection"
   c, addr = s.accept()
   print 'Got connection from', addr

   #Inner loop handles the token exchange
   while True:
      resp = c.recv(1024)

      print resp

      resp_list = resp.split('.')

      if resp_list[0] == "JOIN":
   		#check if in swarm
         process = subprocess.Popen(['docker', 'info'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
         output = subprocess.check_output(('grep', 'Swarm'), stdin=process.stdout)

         if "Swarm: inactive" in output:
            c.send("REQ_token")
         else:
            c.send("BUSY")
            break
     #if response is a token
      elif resp_list[0] == "token":
         #join the swarm with the token
         manager_adress = addr[0] + ":2377"
         man_addr_list = []
         man_addr_list.append(manager_adress)

         worker_join_token = resp_list[1]
         worker_join_token = worker_join_token.strip()

         print "This is the token i will try"
         print worker_join_token

         print manager_adress

         subprocess.call(['docker', 'swarm', 'join', '--token', worker_join_token, manager_adress])

         print "Joined swarm, all worked"
         print resp_list[1]
         c.send("ACK")
         break

   c.close()

