# Enforcing Causal Consistency in Distributed Systems

## Overview:

When a distributed system is composed of multiple servers/data centers and
multiple clients. It might encounter issues related to ordering. The messages
traveling through the Internet might have arrived at the replicated servers in
unexpected order. If these messages are causally related, then the state of the
servers would suffer from inconsistency.

The system implemented is aimed at solving the problem mentioned. The
servers commit replicated requests in a causal order, allowing the distributed
system to stay causally consistent.

In order to demonstrate, we simulate the datacenters with a key-value store
and a few clients. The data centers communicate among themselves.

All components in our application communicate with each other by the
means of JSON.

## Protocol Definition:

The communication between all components are handled by the JSON Data format, and the communication occurs over TCP Sockets.

Each chunk of data exchange consists of two aspects.

1. Headers: These provide meta information about the actual content of the chunk being sent.
2. Body: This will be followed by the actual body of the request or response.

Every exchange of data consists of a Request-Response exchange between two components.

For example:

Request:

```
LENGTH=1234
{ “type”: “q”, “s”: “registerPeer”, “m”: {“ip”: “192.168.1.1”, “port”: 8080}}
```


Response:

```
LENGTH=1234
{ “type”: “r”, “s”: “registerPeer”, “m”: {“ack”: true}}
```

The headers provide the LENGTH information for the complete response body to be read. Followed by the body of the LENGTH bytes provided.

Every request and response body is a dictionary containing the following details:

```
{
    “type”: “r”,  // r - for Response ; q - for Request.
    “s”: “registerPeer”, // Subject of the request, or response. Provides context about which dispatcher to call.
    “m”: {...} // Message body based on the subject. Contains any data relevant to the request or response.
}
```

Based on this the following we have the following request types:

 - ReplicatedWrite: Handles Replicated write requests from other servers.
 - Register: Connection registration request between two datacenters

All the client and server methods described above are implemented for the purpose of demonstration of Causal Consistency using Lamport clock.


## Dependencies:

We only require Golang  (https://golang.org/) v1.17 for this project. Binaries for running this on various operating systems are also provided.
Along with this, we use the following dependencies:

Tablewriter (https://github.com/olekukonko/tablewriter)  - Allows the program to format the printed output as a table.

Note: Although Golang is used, only the low level system constructs such as sockets are utilized for the development of this project.


## Repository Structure:

The repository consists of two files:

 - causal.go: Contains the codebase
 - Go.mod & go.sum: Compile Dependencies
 - Makefile: Makefile used for building the application

## Building:

Assuming you have the golang-1.17 or greater installed, you can run:

```sh
> make build
```
