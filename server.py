from socket import AF_INET, socket, SOCK_STREAM
from threading import Thread
import bisect
import hashlib
import pandas as pd

def get_machine(clients_hash,key):
    '''Returns the number of the machine which key gets sent to.'''
    # edge case where we cycle past hash value of 1 and back to 0.
    if key > clients_hash[-1][1]: return clients_hash[0][0]
    hash_values = [r[1] for r in clients_hash]

    index = bisect.bisect_left(hash_values,key)
    return clients_hash[index][0]

def my_hash(key):
  '''my_hash(key) returns a hash in the range [0,1).'''
  return (int(hashlib.md5(key).hexdigest(),16) % 1000000)/1000000.0


keys = [my_hash("key1".encode()),my_hash("key2".encode()),my_hash("key3".encode()),my_hash("key4".encode()),my_hash("key5".encode()),my_hash("key6".encode()),my_hash("key7".encode()),my_hash("key8".encode()),my_hash("key9".encode()),my_hash("key10".encode())]
keys.sort()
print(keys)

def accept_incoming_connections():
    """Sets up handling for incoming clients."""
    while True:
        client, client_address = SERVER.accept()
        print("%s:%s has connected." % client_address)
        client.send(bytes("Greetings from the cave! Now type your name and press enter!", "utf8"))
        addresses[client] = client_address
        Thread(target=handle_client, args=(client,)).start()
        print(client)

def handle_client(client):  # Takes client socket as argument.
    """Handles a single client connection."""

    name = client.recv(BUFSIZ).decode("utf8")
    welcome = 'Welcome %s! If you ever want to quit, type {quit} to exit.' % name
    client.send(bytes(welcome, "utf8"))
    msg = "%s has joined" % name
    broadcast(bytes(msg, "utf8"))
    clients[client] = name
    clients_hash.append((name,my_hash(name.encode())))
    clients_hash.sort(key = lambda x: x[1])
    print(clients_hash)
    keydict[name] = []

    client.send("Hash list:".encode())

    if(len(clients)>1):
        index = [i for i,h in enumerate(clients_hash) if h[0] == name]
        index = index[0]
        curr_node = clients_hash[(index+1)%len(clients_hash)][0]
        print("Keys : ")
        print(keydict[curr_node])
        for key in keydict[curr_node]:
            print(key," is mapped to ", get_machine(clients_hash,key), " with hash ", clients_hash[index][1])
            new_node = get_machine(clients_hash,key)
            if(new_node!=curr_node):
                keydict[new_node].append(key)
        
        for key in keydict[name]:
            if key in keydict[curr_node]:
                keydict[curr_node].remove(key)

        i= [i for i in clients if clients[i]==curr_node]
        i = i[0]
        
        for key in keydict[curr_node]:
            i.send((str(key)+"\n").encode())

        for key in keydict[name]:
            client.send((str(key)+"\n").encode())
    else:
        for key in keys:
            print(key," is mapped to ", get_machine(clients_hash,key))
            keydict[name].append(key) 
            client.send((str(key)+"\n").encode())
    
    df = pd.DataFrame({ key:pd.Series(value) for key, value in keydict.items() })
    print(df)
    while True:
        msg = client.recv(BUFSIZ)
        if msg != bytes("{quit}", "utf8"):
            broadcast(msg, name+": ")
        else:
            #client.send(bytes("{quit}", "utf8"))
            client.close()
            del clients[client]
            if(len(clients_hash)>1):
                index = [i for i,h in enumerate(clients_hash) if h[0] == name]
                index = index[0]
                index_next = (index+1)%len(clients_hash)
                print("index : ",index)
                print("index_next : ",index_next)

                i= [i for i in clients if clients[i]==clients_hash[index_next][0]]
                i = i[0]
                i.send("Hash list:".encode())
                for key in keydict[clients_hash[index][0]]:
                    keydict[clients_hash[index_next][0]].append(key)
                for key in keydict[clients_hash[index_next][0]]:
                    i.send((str(key)+"\n").encode())
                    i.send(str("\n").encode())
                del keydict[name]
                del clients_hash[index]
            else:
                del clients_hash[0]
                del keydict[name]
            broadcast(bytes("%s has left" % name, "utf8"))
            print(name+" has left")
            df = pd.DataFrame({ key:pd.Series(value) for key, value in keydict.items() })
            print(df)
            break


def broadcast(msg, prefix=""):  # prefix is for name identification.
    """Broadcasts a message to all the clients."""

    for sock in clients:
        try:
            sock.send(bytes(prefix, "utf8")+msg)
        except:
            continue

        
clients = {}
addresses = {}
clients_hash = []
keydict = {}

HOST = ''
PORT = 33000
BUFSIZ = 1024
ADDR = (HOST, PORT)

SERVER = socket(AF_INET, SOCK_STREAM)
SERVER.bind(ADDR)

if __name__ == "__main__":
    SERVER.listen(5)
    print("Waiting for connection...")
    ACCEPT_THREAD = Thread(target=accept_incoming_connections)
    ACCEPT_THREAD.start()
    ACCEPT_THREAD.join()
    
    SERVER.close()