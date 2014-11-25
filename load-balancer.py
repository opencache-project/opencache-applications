#!/usr/bin/env python2.7

"""Load balancer application for OpenCache."""

import json
import random
import urllib
import optparse
import time

class Node(object):
    """Represents a single nodes capacity and load."""
    id_ = None
    expr = []
    load = {}
    required_expr = []
    online = True

    def __init__(self, **kwargs):
        for key, val in kwargs.items():
            setattr(self, key, val)

def _load_file(file):
    """Load the JSON configuration file."""
    file_handle = open(file)
    return json.load(file_handle)

def _parse_config(config):
    """Parse the JSON configuration into objects."""
    nodes = {}
    for key, dict_ in config.items():
        for id_, value in dict_.items():
            if id_ not in nodes:
                nodes[id_] = Node(id_=id_)
            setattr(nodes[id_], key, value)
    for node in nodes.values():
        print node.required_expr
    return nodes

def _do_opencache_call(method, options, node, expr, call_id=None):
    """Make a JSON-RPC call to the OpenCache controller."""
    if call_id is None:
        call_id = random.randint(1, 999)
    params = {'node': str(node), 'expr': str(expr)}
    url = "http://%s:%s" % (options.host, options.port)
    try:
        post_data = json.dumps({"id": call_id, "method": str(method),
                               "params": params, "jsonrpc": "2.0"})
    except Exception as exception:
        print "[ERROR] Could not encode JSON: %s" % exception
    try:
        response_data = urllib.urlopen(url, post_data).read()
        print "[INFO] Sent request: %s" % post_data
        try:
            response_json = json.loads(response_data)
            if response_json['id'] == str(call_id):
                print "[INFO] Received response: %s" % response_json
                return response_json
            else:
                print "[ERROR] Mismatched call ID for response: %s" % response_json
                raise IOError("Mismatched call ID for response: %s" % response_json)
        except Exception as exception:
            print "[ERROR] Could not decode JSON from OpenCache node response: %s" % exception
    except IOError as exception:
        #print "[ERROR] Could not connect to OpenCache instance: %s" % exception
        return {}

def _update(options, nodes):
    """Update node data from the nodes themselves."""
    for id_, node in nodes.items():
        _do_opencache_call('refresh', options, id_, '*')
        try:
            result = _do_opencache_call('stat', options, id_, '*')['result']
            node_id_seen = int(result['node_id_seen'])
            if node_id_seen == 1:
                node.online = True
            elif node_id_seen > 1:
                print "[ERROR] That's weird, seen more than 1 node ID in the result."
            node.load['cache_miss'] = int(result['total_cache_miss'])
            node.load['cache_miss_size'] = int(result['total_cache_miss_size'])
            node.load['cache_hit'] = int(result['total_cache_hit'])
            node.load['cache_hit_size'] = int(result['total_cache_hit_size'])
            node.load['cache_object'] = int(result['total_cache_object'])
            node.load['cache_object_size'] = int(result['total_cache_object_size'])
            node.expr = list(result['expr_seen'])
        except Exception as e:
            print "[ERROR] %s" % str(e)

def _check_thresholds(nodes):
    """Check to see which metrics are exceeding their thresholds."""
    overloaded = []
    for node in nodes.values():
        for metric in node.capacity.keys():
            if node.load[metric] > node.capacity[metric]:
                overloaded.append(({'node': node, 'metric': metric}))
    return overloaded

def _find_node_to_move_to(nodes, metric='', load=0):
    """
    Find the most appropriate node to move the load to.

    List of nodes is randomised to create a 'round-robin' effect.

    """
    keys = list(nodes.keys())
    random.shuffle(keys)
    nodes = [(key, nodes[key]) for key in keys]
    for _, node in nodes:
        if node.online:
            if metric is not '' or load is not 0:
                if (node.load[metric] < node.capacity[metric] + int(load)):
                    return node
            else:
                return node
    return None

def _find_expr_to_move(options, node, metric):
    """ Find the most appropriate expression to move on the given node."""
    load = []
    key = 'total_' + str(metric)
    for expr in node.expr:
        _do_opencache_call('refresh', options, node.id_, expr)
        try:
            result = _do_opencache_call('stat', options, node.id_, expr)['result']
            load.append({'expr': expr, 'load': int(result[key])})
        except Exception as e:
            print "[ERROR] %s" % str(e)
    load.sort(key=lambda tup: tup['load'])
    to_move = node.load[metric] - node.capacity[metric]
    for item in load:
        if item['load'] > to_move:
            return item['expr'], item['load']
    return None, None

def _move_expr(options, expr, to_node, from_node=None):
    """Call the OpenCache API to move the content between nodes."""
    if from_node:
        _do_opencache_call('pause', options, from_node, expr)  #TODO: check for success of earlier command
        _do_opencache_call('start', options, to_node, expr)
        _do_opencache_call('stop', options, from_node, expr)
    else:
        _do_opencache_call('start', options, to_node, expr)


def _do_load_balancing(options, nodes):
    overloaded = _check_thresholds(nodes)
    for item in overloaded:
        expr_to_move, load = _find_expr_to_move(options, item['node'], item['metric'])
        if not expr_to_move:
            print "[ERROR] No expression found to move from overloaded node."
            break
        node_to_move_to = _find_node_to_move_to(nodes, item['metric'], load)
        if not node_to_move_to:
            print "[ERROR] No node found to move load to."
            break
        _move_expr(options, expr_to_move, item['node'].id_,
                   node_to_move_to.id_)

def _check_required(nodes):
    missing = []
    for node in nodes.values():
        if node.online:
            diff = set.difference(set(node.required_expr), set(node.expr))
        else:
            diff = node.required_expr
        if len(diff):
            missing.append({'node': node, 'expr': list(diff)})
    return missing

def _amend_required_expr(node_to_move_to, node_to_move_from, expr):
    node_to_move_to.required_expr.append(expr)
    node_to_move_from.required_expr.remove(expr)

def _do_fail_checking(options, nodes):
    missing = _check_required(nodes)
    for item in missing:
        for expr in item['expr']:
            node_to_move_to = _find_node_to_move_to(nodes)
            _move_expr(options, expr, node_to_move_to)
            _amend_required_expr(node_to_move_to, item['node'], expr)

def _parse_options():
    """Parse the command line options given."""
    parser = optparse.OptionParser()
    parser.add_option("-i", "--hostname", dest="host", default='127.0.0.1',
                      help="hostname of OpenCache controller")
    parser.add_option("-p", "--port", dest="port", default='49001',
                      help="port number of the OpenCache JSON-RPC interface")
    parser.add_option("-c", "--config", dest="config",
                      help="path of load balancer configuration")
    parser.add_option("-d", "--delay", dest="delay", default=10,
                      help="delay between load balancing operations")
    parser.add_option("--no-fail", dest="fail", default=True,
                      action="store_false", help="inhibit failover protection")
    parser.add_option("--no-load", dest="load", default=True,
                      action="store_false", help="inhibit load balancing")
    return parser.parse_args()

if __name__ == '__main__':
    options, _ = _parse_options()
    if options.config:
        config = _load_file(options.config)
    else:
        print "[ERROR] No configuration file given."
        exit()
    nodes = _parse_config(config)
    while True:
        _update(options, nodes)
        if options.fail:
            _do_fail_checking(options, nodes)
        if options.load:
            _do_load_balancing(options, nodes)
        time.sleep(float(options.delay))
