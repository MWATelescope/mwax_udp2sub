// this thing just grabs 10k packets from each coarse channel, and does basic validation on the headers of each packet received.

#define _GNU_SOURCE

#include <sys/types.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include <sys/socket.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>

#define UDP_num_slots 4000

#pragma pack(push, 1)  // We're writing this header into all our packets, so we want/need to force the compiler not to add it's own idea of structure padding

typedef struct mwa_udp_packet {  // Structure format for the MWA data packets

  uint8_t packet_type;   // Packet type (cf MWA_PACKET_TYPE_* above)
  uint8_t freq_channel;  // The current coarse channel frequency number [0 to 255 inclusive].  This number is available from the header, but repeated here to simplify archiving and
                         // monitoring multiple frequencies.
  uint16_t rf_input;     // maps to tile/antenna and polarisation (LSB is 0 for X, 1 for Y)

  uint32_t GPS_time;  // [second] GPS time of packet (bottom 32 bits only)

  uint16_t subsec_time;  // count from 0 to PACKETS_PER_SEC - 1 (from header)  Typically PACKETS_PER_SEC is 625 in MWA
  uint8_t spare1;        // spare padding byte.  Reserved for future use.
  uint8_t edt2udp_id;    // Which edt2udp instance generated this packet and therefore a lookup to who to ask for a retransmission (0 implies no retransmission possible)

  uint16_t edt2udp_token;  // value to pass back to edt2udp instance to help identify source packet.  A token's value is constant for a given source rf_input and freq_channel for
                           // entire sub-observation
  uint8_t spare2[2];       // Spare bytes for future use

  uint16_t volts[2048];  // 2048 complex voltage samples.  Each one is 8 bits real, followed by 8 bits imaginary but we'll treat them as a single 16 bit 'bit pattern'

} mwa_udp_packet_t;

struct mmsghdr *msgvecs;
struct iovec *iovecs;
mwa_udp_packet_t *UDPbuf;

struct {
  char *multicast_ip;
  int UDPport;
  char *local_if;
} conf;

int UDP_recv_complete = 0;
void UDP_recv(int channel) {
  if (channel < 1 || channel > 24) return;

  char ip[] = "239.255.90.11";
  snprintf(ip, sizeof(ip), "239.255.90.%d", channel);

  conf.local_if     = "192.168.90.225";
  conf.multicast_ip = ip;
  conf.UDPport      = 59000 + channel;

  // printf("Set up to receive from multicast %s:%d on interface %s\n", conf.multicast_ip, conf.UDPport, conf.local_if);

  struct sockaddr_in addr;  // Standard socket setup stuff needed even for multicast UDP
  memset(&addr, 0, sizeof(addr));

  int fd;
  struct ip_mreq mreq;

  if ((fd = socket(AF_INET, SOCK_DGRAM, 0)) == -1) {  // create what looks like an ordinary UDP socket
    perror("socket");
    return;  // pthread_exit(NULL);
  }

  addr.sin_family      = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_ANY);
  addr.sin_port        = htons(conf.UDPport);

  if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &(int){1}, sizeof(int)) == -1) {
    perror("setsockopt SO_REUSEADDR");
    close(fd);
    return;  // pthread_exit(NULL);
  }

  if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) == -1) {  // bind to the receive address
    perror("bind");
    close(fd);
    return;  // pthread_exit(NULL);
  }

  mreq.imr_multiaddr.s_addr = inet_addr(conf.multicast_ip);  // use setsockopt() to request that the kernel join a multicast group
  mreq.imr_interface.s_addr = inet_addr(conf.local_if);

  if (setsockopt(fd, IPPROTO_IP, IP_ADD_MEMBERSHIP, &mreq, sizeof(mreq)) == -1) {
    perror("setsockopt IP_ADD_MEMBERSHIP");
    close(fd);
    return;  // pthread_exit(NULL);
  }

  //--------------------------------------------------------------------------------------------------------------------------------------------------------

  struct mmsghdr *UDP_first_empty_ptr = msgvecs;  // Set our first empty pointer to the address of the 0th element in the array

  int retval;
  int desired = 10000;
  int ok      = 0;
  int not_ok  = 0;

  while (desired > 0) {
    usleep(2100);
    retval = recvmmsg(fd, UDP_first_empty_ptr, UDP_num_slots < desired ? UDP_num_slots : desired, MSG_WAITFORONE, NULL);

    for (int i = 0; i < retval; i++) {
      if (UDPbuf[i].packet_type == 0x20 && ntohs(UDPbuf[i].rf_input) > 0 && ntohl(UDPbuf[i].GPS_time) > 1414834700 && ntohl(UDPbuf[i].GPS_time) < 1514834700) {
        ok += 1;
      } else {
        if (not_ok < 1) {
          printf("     :UDPbuf[i].packet_type = %02x\n", UDPbuf[i].packet_type);
          printf("     :UDPbuf[i].GPS_time    = %d\n", ntohl(UDPbuf[i].GPS_time));
          printf("     :UDPbuf[i].rf_input    = %d\n\n", ntohs(UDPbuf[i].rf_input));
        }
        not_ok += 1;
      }
    }
    desired -= retval;
  }
  retval = ok + not_ok;

  printf("chan %2d: %8d packets received, ", channel, retval);
  printf("%5.2f%% of packets ok  (%d bad)\n", ok * 100.0f / retval, retval - ok);

  mreq.imr_multiaddr.s_addr = inet_addr(conf.multicast_ip);
  mreq.imr_interface.s_addr = inet_addr(conf.local_if);

  if (setsockopt(fd, IPPROTO_IP, IP_DROP_MEMBERSHIP, &mreq, sizeof(mreq)) == -1) {  // Say we don't want multicast packets any more.
    perror("setsockopt IP_DROP_MEMBERSHIP");
    close(fd);
    return;  // pthread_exit(NULL);
  }

  close(fd);  // Close the file descriptor for the port now we're about the leave

  UDP_recv_complete = 1;
  return;  // pthread_exit(NULL);
}

void *calloc_or_die(size_t nmemb, size_t size) {
  void *res = calloc(nmemb, size);
  if (!res) {
    fprintf(stderr, "calloc failed\n");  // log a message
    fflush(stderr);
    exit(EXIT_FAILURE);  // and give up!
  }
  return res;
}

int main() {
  msgvecs = calloc_or_die(UDP_num_slots, sizeof(struct mmsghdr));  // NB Make twice as big an array as the number of actual UDP packets we are going to buffer
  iovecs  = calloc_or_die(UDP_num_slots, sizeof(struct iovec));    // NB Make the *same* number of entries as the number of actual UDP packets we are going to buffer
  UDPbuf  = calloc_or_die(UDP_num_slots,
                          sizeof(mwa_udp_packet_t));  // NB Make the *same* number of entries as the number of actual UDP packets we are going to buffer

  for (int loop = 0; loop < UDP_num_slots; loop++) {
    iovecs[loop].iov_base            = &UDPbuf[loop];
    iovecs[loop].iov_len             = sizeof(mwa_udp_packet_t);
    msgvecs[loop].msg_hdr.msg_iov    = &iovecs[loop];  // Populate the first copy
    msgvecs[loop].msg_hdr.msg_iovlen = 1;
  }

  for (int i = 1; i < 25; i++) {
    if (i != 22) UDP_recv(i);
  }

  free(UDPbuf);  // these frees are only here to make the linter happy.
  free(iovecs);
  free(msgvecs);
  return 0;
}