"""
Implementation of the lower level of GALP, handles routing, priority and
counters.
"""

import logging

class LowerProtocol:
    def __init__(self, name=None, router=False):
        """
        Args:
            name: short string to include in log messages.
            router: True if sending function should move a routing id in the
                first routing segment. Default False.
        """
        self.proto_name = name
        self.router=router

    # Callbacks
    # =========

    def send_message(self, msg):
        """Callback method to send a message just built.

        This is the only callback that is required to be implemented.

        Args:
            msg: the fully assembled message to pass to the transport.
        """
        raise NotImplementedError("You must override the send_message method "
            "when subclassing a Protocol object.")

    def on_verb(self, route, msg_body):
        """
        Args:
            route: a tuple (incoming_route, forwarding route)
            msg_body: all the message parts starting with the galp verb
        """
        logging.warning("No upper protocol callback.")

    # Main public methods
    # ===================

    def on_message(self, msg):
        """
        Parses the lower part of a GALP message,
        then calls the upper protocol with the parsed message.
        """
        self.validate(len(msg) > 0, 'Empty message')
        msg_body, route_parts = self._get_routing_parts(msg)
        route = self._split_route(route_parts)

        logging.info('<- %s[%s] %s',
            self.proto_name +" " if self.proto_name else "",
            route_parts[0].hex() if route_parts else "",
            msg_body[0].decode('ascii'))
        logging.debug("%d routing part(s)", len(route_parts))

        return self.on_verb(route, msg_body)

    def send_message_to(self, route, msg_body):
        """Callback method to send a message just built.

        Wrapper around send_message that concats route and message.
        """

        incoming_route, forward_route = route
        if not forward_route:
            # Assuming we want to reply
            incoming_route, forward_route = forward_route, incoming_route
        if self.router:
            # If routing, we need an id, we take it from the forward segment
            next_hop, *forward_route = forward_route
            incoming_route = [next_hop, *incoming_route]
        route_parts = incoming_route + forward_route
        logging.info('-> %s[%s] %s',
            self.proto_name +" " if self.proto_name else "",
            route_parts[0].hex() if route_parts else "",
            msg_body[0].decode('ascii'))
        return self.send_message(self.build_message(route_parts, msg_body))

    def default_route(self):
        """
        Route to pass to send methods to simply send a message a connected
        unnamed peer
        """
        # This is a tuple (incoming, forward) with both routes set to the empty
        # route
        return ([], [])

    # Internal parsing utilities
    # ==========================

    def _get_routing_parts(self, msg):
        """
        Parses and returns the routing part of `msg`, and the rest.

        Can be overloaded to handle different routing strategies.
        """
        route_parts = []
        while msg and msg[0]:
            route_parts.append(msg[0])
            msg = msg[1:]
        # Discard empty frame
        msg = msg[1:]
        self.validate(len(msg) > 0, route_parts, 'Empty message with only routing information')
        return msg, route_parts

    def _split_route(self, route_parts):
        """
        With only one part, it is interpreted as incoming with an empty forward
        """
        incoming_route = route_parts[:1]
        forward_route = route_parts[1:]
        return incoming_route, forward_route


    def build_message(self, route_parts, msg_body):
        return route_parts + [b''] + msg_body

