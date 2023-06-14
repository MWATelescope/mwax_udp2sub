
void *heartbeat()
{
    unsigned char ttl = MONITOR_TTL;
    struct sockaddr_in addr;
    struct in_addr localInterface;

    int monitor_socket;

    // create what looks like an ordinary UDP socket
    if ( ( monitor_socket = socket( AF_INET, SOCK_DGRAM, 0) ) < 0) {
      perror("socket");
      terminate = true;
      pthread_exit(NULL);
    }

    // set up destination address
    memset( &addr, 0, sizeof(addr) );
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = inet_addr( MONITOR_IP );
    addr.sin_port = htons( MONITOR_PORT );

    setsockopt( monitor_socket, IPPROTO_IP, IP_MULTICAST_TTL, &ttl, sizeof(ttl) );

    char loopch = 0;

    if (setsockopt( monitor_socket, IPPROTO_IP, IP_MULTICAST_LOOP, (char *) &loopch, sizeof(loopch) ) < 0) {
      perror("setting IP_MULTICAST_LOOP:");
      close( monitor_socket );
      terminate = true;
      pthread_exit(NULL);
    }

    // Set local interface for outbound multicast datagrams. The IP address specified must be associated with a local, multicast-capable interface.
    localInterface.s_addr = inet_addr( conf.monitor_if );

    if (setsockopt( monitor_socket, IPPROTO_IP, IP_MULTICAST_IF, (char *) &localInterface, sizeof(localInterface) ) < 0) {
      perror("setting local interface");
      close( monitor_socket );
      terminate = true;
      pthread_exit(NULL);
    }

   monitor.version = BUILD;

    while(!terminate) {
      if (sendto( monitor_socket, &monitor, sizeof(monitor), 0, (struct sockaddr *) &addr, sizeof(addr) ) < 0) {
        printf( "\nFailed to send monitor packet" );
        fflush(stdout);
      }
      usleep(1000000);
    }

    printf("\nStopping heartbeat");
    pthread_exit(NULL);
}