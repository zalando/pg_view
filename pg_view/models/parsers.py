import os
import re
import socket

from pg_view.loggers import logger


class ProcNetParser():
    """ Parse /proc/net/{tcp,tcp6,unix} and return the list of address:port
        pairs given the set of socket descriptors belonging to the object.
        The result is grouped by the socket type in a dictionary.
    """
    NET_UNIX_FILENAME = '/proc/net/unix'
    NET_TCP_FILENAME = '/proc/net/tcp'
    NET_TCP6_FILENAME = '/proc/net/tcp6'

    def __init__(self):
        self.reinit()

    def reinit(self):
        self.sockets = {}
        self.unix_socket_header_len = 0
        # initialize the sockets hash with the contents of unix
        # and tcp sockets. tcp IPv6 is also read if it's present
        for fname in (ProcNetParser.NET_UNIX_FILENAME, ProcNetParser.NET_TCP_FILENAME):
            self.read_socket_file(fname)
        if os.access(ProcNetParser.NET_TCP6_FILENAME, os.R_OK):
            self.read_socket_file(ProcNetParser.NET_TCP6_FILENAME)

    @staticmethod
    def _hex_to_int_str(val):
        return str(int(val, 16))

    @staticmethod
    def _hex_to_ip(val):
        newval = format(socket.ntohl(int(val, 16)), '08X')
        return '.'.join([str(int(newval[i: i + 2], 16)) for i in range(0, 8, 2)])

    @staticmethod
    def _hex_to_ipv6(val):
        newval_list = [format(socket.ntohl(int(val[x: x + 8], 16)), '08X') for x in range(0, 32, 8)]
        return ':'.join([':'.join((x[:4], x[4:])) for x in newval_list])

    def match_socket_inodes(self, inodes):
        """ return the dictionary with socket types as strings,
            containing addresses (or unix path names) and port
        """
        result = {}
        for inode in inodes:
            if inode in self.sockets:
                addr_tuple = self.parse_single_line(inode)
                if addr_tuple is None:
                    continue
                socket_type = addr_tuple[0]
                if socket_type in result:
                    result[socket_type].append(addr_tuple[1:])
                else:
                    result[socket_type] = [addr_tuple[1:]]
        return result

    def read_socket_file(self, filename):
        """ read file content, produce a dict of socket inode -> line """
        socket_type = filename.split('/')[-1]
        try:
            with open(filename) as fp:
                data = fp.readlines()
        except os.error as e:
            logger.error('unable to read from {0}: OS reported {1}'.format(filename, e))
        # remove the header
        header = (data.pop(0)).split()
        if socket_type == 'unix':
            self.unix_socket_header_len = len(header)
        indexes = [i for i, name in enumerate(header) if name.lower() == 'inode']
        if len(indexes) != 1:
            logger.error('attribute \'inode\' in the header of {0} is not unique or missing: {1}'.format(
                filename, header))
        else:
            inode_idx = indexes[0]
            if socket_type != 'unix':
                # for a tcp socket, 2 pairs of fields (tx_queue:rx_queue and tr:tm->when
                # are separated by colons and not spaces)
                inode_idx -= 2
            for line in data:
                fields = line.split()
                inode = int(fields[inode_idx])
                self.sockets[inode] = [socket_type, line]

    def parse_single_line(self, inode):
        """ apply socket-specific parsing rules """
        result = None
        (socket_type, line) = self.sockets[inode]
        if socket_type == 'unix':
            # we are interested in everything in the last field
            # note that it may contain spaces or other separator characters
            fields = line.split(None, self.unix_socket_header_len - 1)
            socket_path = fields[-1]
            # check that it looks like a PostgreSQL socket
            match = re.search(r'(.*?)/\.s\.PGSQL\.(\d+)$', socket_path)
            if match:
                # path - port
                result = (socket_type,) + match.groups(1)
            else:
                logger.warning(
                    'unix socket name is not recognized as belonging to PostgreSQL: {0}'.format(socket_path))
        else:
            address_port = line.split()[1]
            (address_hex, port_hex) = address_port.split(':')
            port = self._hex_to_int_str(port_hex)
            if socket_type == 'tcp6':
                address = self._hex_to_ipv6(address_hex)
            elif socket_type == 'tcp':
                address = self._hex_to_ip(address_hex)
            else:
                logger.error('unrecognized socket type: {0}'.format(socket_type))
            result = (socket_type, address, port)
        return result
