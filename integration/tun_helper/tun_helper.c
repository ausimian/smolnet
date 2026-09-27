/*
 * tun_helper: relays raw IP packets between a TUN device and a BEAM port.
 *
 *     tun_helper --device NAME   attach to TUN device NAME (IFF_TUN | IFF_NO_PI)
 *     tun_helper --loopback      open no device; echo every packet back
 *
 * Both directions are framed as Erlang's {packet, 2}: a 16-bit big-endian
 * length, then that many bytes, the first of which is the frame's type.
 * Integers are big-endian.
 *
 *   stdin   0 PACKET  a raw IP packet to write to the device
 *           1 CREDIT  <<packets:32>>: read that many more device packets
 *   stdout  0 PACKET  a raw IP packet read from the device
 *           1 ACK     <<packets:32, bytes:32, dropped:32, rx_dropped:32>>:
 *                     the PACKET frames consumed since the last ACK, their
 *                     total length, how many the device refused, and (in
 *                     loopback mode) how many echoes had no credit
 *           2 READY   the device name, sent once the device is open
 *
 * A frame is acknowledged once write(2) on the device has returned, so the
 * link can grant egress credit back to its stack from real write completion.
 * In the other direction the helper reads the device only while it holds
 * credit from the link, so a BEAM that falls behind leaves packets queued in
 * the kernel, which drops them from the device's own queue as a real network
 * interface would, rather than in an unbounded mailbox.
 *
 * The helper exits 0 when stdin reaches end of file (the port closed or the
 * BEAM exited) and non-zero, with a message on stderr, on any other failure.
 * Loopback mode needs no privileges and no device; it exists so the port
 * protocol and the link's credit loop can be checked anywhere.
 */

#define _DEFAULT_SOURCE

#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#if defined(__linux__)
#include <linux/if.h>
#include <linux/if_tun.h>
#include <sys/ioctl.h>
#endif

/* The largest {packet, 2} frame, and so the largest packet stdin can carry. */
#define FRAME_MAX 65535u
/* Outbound frames spend one byte on their type. */
#define PACKET_MAX (FRAME_MAX - 1u)
/* Device reads per wake-up, so stdin is never starved by a busy device. */
#define READ_BURST 64
/* How long a device that reports itself full may stay full before a packet
 * written to it counts as dropped. */
#define WRITE_WAIT_MS 100

enum { FRAME_PACKET = 0, FRAME_ACK = 1, FRAME_READY = 2 };
enum { INPUT_PACKET = 0, INPUT_CREDIT = 1 };

/* Device packets the link has room for. */
static uint32_t rx_credit;

static unsigned char in_buf[4 * (2 + FRAME_MAX)];
static size_t in_len;
static unsigned char out_buf[4 * (2 + FRAME_MAX)];
static size_t out_len;
static unsigned char packet_buf[FRAME_MAX];

static void die(const char *format, ...) {
  va_list args;

  fputs("tun_helper: ", stderr);
  va_start(args, format);
  vfprintf(stderr, format, args);
  va_end(args);
  fputc('\n', stderr);
  exit(1);
}

static void write_all(int fd, const unsigned char *data, size_t length) {
  while (length > 0) {
    ssize_t written = write(fd, data, length);

    if (written < 0) {
      if (errno == EINTR) {
        continue;
      }

      /* The BEAM closed its end of the pipe: nobody is left to serve. */
      if (errno == EPIPE) {
        exit(0);
      }

      die("write to stdout: %s", strerror(errno));
    }

    data += written;
    length -= (size_t)written;
  }
}

static void flush_output(void) {
  write_all(STDOUT_FILENO, out_buf, out_len);
  out_len = 0;
}

static void emit(unsigned char type, const unsigned char *body, size_t length) {
  size_t frame = length + 1;

  if (out_len + 2 + frame > sizeof out_buf) {
    flush_output();
  }

  out_buf[out_len++] = (unsigned char)(frame >> 8);
  out_buf[out_len++] = (unsigned char)(frame & 0xff);
  out_buf[out_len++] = type;
  memcpy(out_buf + out_len, body, length);
  out_len += length;
}

static void put_u32(unsigned char *target, uint32_t value) {
  target[0] = (unsigned char)(value >> 24);
  target[1] = (unsigned char)(value >> 16);
  target[2] = (unsigned char)(value >> 8);
  target[3] = (unsigned char)value;
}

static uint32_t get_u32(const unsigned char *source) {
  return ((uint32_t)source[0] << 24) | ((uint32_t)source[1] << 16) |
         ((uint32_t)source[2] << 8) | (uint32_t)source[3];
}

static void add_credit(uint32_t packets) {
  rx_credit = packets > UINT32_MAX - rx_credit ? UINT32_MAX : rx_credit + packets;
}

static void nap_ms(long milliseconds) {
  struct timespec delay = {0, milliseconds * 1000000L};

  while (nanosleep(&delay, &delay) < 0 && errno == EINTR) {
  }
}

#if defined(__linux__)
static int open_device(const char *name, char *actual, size_t actual_size) {
  struct ifreq request;
  int fd;
  int flags;

  if (strlen(name) >= IFNAMSIZ) {
    die("device name too long: %s", name);
  }

  fd = open("/dev/net/tun", O_RDWR | O_CLOEXEC);

  if (fd < 0) {
    die("open /dev/net/tun: %s", strerror(errno));
  }

  memset(&request, 0, sizeof request);
  request.ifr_flags = IFF_TUN | IFF_NO_PI;
  memcpy(request.ifr_name, name, strlen(name));

  if (ioctl(fd, TUNSETIFF, &request) < 0) {
    die("attach to %s: %s", name, strerror(errno));
  }

  flags = fcntl(fd, F_GETFL);

  if (flags < 0 || fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
    die("make %s non-blocking: %s", name, strerror(errno));
  }

  snprintf(actual, actual_size, "%s", request.ifr_name);
  return fd;
}
#else
static int open_device(const char *name, char *actual, size_t actual_size) {
  (void)actual;
  (void)actual_size;
  die("TUN devices are supported on Linux only (asked for %s); use --loopback", name);
  return -1;
}
#endif

/* Returns 1 if the device took the packet and 0 if it refused it. */
static int write_device(int fd, const unsigned char *packet, size_t length) {
  int waited = 0;

  for (;;) {
    ssize_t written = write(fd, packet, length);

    if (written >= 0) {
      /* A TUN write is one packet: a short write cannot be completed. */
      return (size_t)written == length;
    }

    if (errno == EINTR) {
      continue;
    }

    if ((errno == EAGAIN || errno == EWOULDBLOCK) && !waited) {
      struct pollfd writable = {fd, POLLOUT, 0};

      waited = 1;

      if (poll(&writable, 1, WRITE_WAIT_MS) > 0) {
        continue;
      }
    }

    /* EIO (device down), EINVAL (malformed packet), ENOBUFS and the like:
     * the device refused the packet, as a real network would drop it. */
    return 0;
  }
}

/* Returns 0 once stdin reaches end of file. */
static int relay_stdin(int device, int loopback) {
  ssize_t count = read(STDIN_FILENO, in_buf + in_len, sizeof in_buf - in_len);
  size_t position = 0;
  uint32_t packets = 0;
  uint32_t bytes = 0;
  uint32_t dropped = 0;
  uint32_t rx_dropped = 0;

  if (count == 0) {
    return 0;
  }

  if (count < 0) {
    if (errno == EINTR || errno == EAGAIN) {
      return 1;
    }

    die("read from stdin: %s", strerror(errno));
  }

  in_len += (size_t)count;

  while (in_len - position >= 2) {
    size_t length = ((size_t)in_buf[position] << 8) | in_buf[position + 1];
    const unsigned char *frame = in_buf + position + 2;

    if (in_len - position < 2 + length) {
      break;
    }

    position += 2 + length;

    if (length == 5 && frame[0] == INPUT_CREDIT) {
      add_credit(get_u32(frame + 1));
    } else if (length >= 1 && frame[0] == INPUT_PACKET) {
      const unsigned char *packet = frame + 1;
      size_t size = length - 1;
      int accepted = size > 0;

      if (accepted && loopback) {
        /* The echo is the device's inbound packet, so it needs credit. */
        if (rx_credit > 0) {
          rx_credit--;
          emit(FRAME_PACKET, packet, size);
        } else {
          rx_dropped++;
        }
      } else if (accepted) {
        accepted = write_device(device, packet, size);
      }

      packets++;
      bytes += (uint32_t)size;
      dropped += accepted ? 0 : 1;
    } else {
      die("unknown input frame of %zu bytes, type %u", length, length ? frame[0] : 0u);
    }
  }

  memmove(in_buf, in_buf + position, in_len - position);
  in_len -= position;

  if (packets > 0) {
    unsigned char ack[16];

    put_u32(ack, packets);
    put_u32(ack + 4, bytes);
    put_u32(ack + 8, dropped);
    put_u32(ack + 12, rx_dropped);
    emit(FRAME_ACK, ack, sizeof ack);
  }

  return 1;
}

static void relay_device(int device) {
  /* Woken by an error or hang-up with no credit to read: do not spin. */
  if (rx_credit == 0) {
    nap_ms(10);
    return;
  }

  for (int burst = 0; burst < READ_BURST && rx_credit > 0; burst++) {
    ssize_t count = read(device, packet_buf, sizeof packet_buf);

    if (count < 0) {
      if (errno == EINTR) {
        continue;
      }

      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        return;
      }

      /* A device that is administratively down reports EIO; wait for it to
       * come back rather than spinning or giving up. */
      if (errno == EIO) {
        nap_ms(10);
        return;
      }

      die("read from device: %s", strerror(errno));
    }

    /* A packet too large to frame cannot be delivered; drop it. */
    if (count > 0 && (size_t)count <= PACKET_MAX) {
      rx_credit--;
      emit(FRAME_PACKET, packet_buf, (size_t)count);
    }
  }
}

static void usage(void) {
  fputs("usage: tun_helper --device NAME | --loopback\n", stderr);
  exit(2);
}

int main(int argc, char **argv) {
  char name[64] = "loopback";
  int loopback = 0;
  int device = -1;

  if (argc == 2 && strcmp(argv[1], "--loopback") == 0) {
    loopback = 1;
  } else if (argc == 3 && strcmp(argv[1], "--device") == 0) {
    device = open_device(argv[2], name, sizeof name);
  } else {
    usage();
  }

  /* A closed stdout must surface as EPIPE, not kill the helper silently. */
  signal(SIGPIPE, SIG_IGN);

  emit(FRAME_READY, (const unsigned char *)name, strlen(name));
  flush_output();

  for (;;) {
    /* Without credit, leave device packets queued in the kernel. */
    short device_events = rx_credit > 0 ? POLLIN : 0;
    struct pollfd fds[2] = {{STDIN_FILENO, POLLIN, 0}, {device, device_events, 0}};
    nfds_t count = loopback ? 1 : 2;

    if (poll(fds, count, -1) < 0) {
      if (errno == EINTR) {
        continue;
      }

      die("poll: %s", strerror(errno));
    }

    if (fds[0].revents & (POLLIN | POLLHUP | POLLERR)) {
      if (!relay_stdin(device, loopback)) {
        flush_output();
        return 0;
      }
    }

    if (!loopback) {
      if (fds[1].revents & POLLNVAL) {
        die("device descriptor closed");
      }

      if (fds[1].revents & (POLLIN | POLLERR | POLLHUP)) {
        relay_device(device);
      }
    }

    flush_output();
  }
}
