#!/usr/bin/python

# ******************************************************************************
# Name: sqs-py-poller
# Description: A simple AWS sqs message poller with configurable logging
# Author: Roy Feintuch (froyke)
#
# Copywrite 2015, Dome9 Security
# www.dome9.com - secure your cloud
# ******************************************************************************

import boto3
from botocore.exceptions import ClientError
import json
import time
import sys
import socket
from configparser import ConfigParser
import logging
import logging.handlers
from datetime import datetime

logger = logging.getLogger('poller')


def run():
    """
    Main function

    :return:
    """
    print("starting SQS poller script")

    """
    Check if we have been asked to run forever via command line
    """
    forever = any("forever" in s for s in sys.argv)

    start = datetime.now()

    # load config file
    config = ConfigParser()
    config.read("./poller.conf")

    # Set up logging
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')

    if config.getboolean('console', 'enabled'):
        console_hdlr = logging.StreamHandler(sys.stdout)
        console_hdlr.setLevel(logging.DEBUG)
        logger.addHandler(console_hdlr)

    if config.getboolean('file_logger', 'enabled'):
        log_path = config.get('file_logger', 'logPath')
        hdlr = logging.FileHandler(log_path)
        hdlr.setFormatter(formatter)
        hdlr.setLevel(logging.INFO)
        logger.addHandler(hdlr)

    if config.getboolean('syslog', 'enabled'):
        host = config.get('syslog', 'host')
        port = config.getint('syslog', 'port')
        syslog_hdlr = logging.handlers.SysLogHandler(address=(host, port), socktype=socket.SOCK_DGRAM)
        syslog_hdlr.setFormatter(formatter)
        syslog_hdlr.setLevel(logging.INFO)
        logger.addHandler(syslog_hdlr)

    max_receive_count = 1
    if config.has_option('poller', 'max_receive_count'):
        max_receive_count = config.getint('poller', 'max_receive_count')

    max_worker_uptime = 60
    if config.has_option('poller', 'max_worker_uptime'):
        max_worker_uptime = config.getint('poller', 'max_worker_uptime')
        if max_worker_uptime == 0:
            forever = True
        else:
            forever = False

    visibility_timeout = 30
    if config.has_option('poller', 'visibility_timeout'):
        visibility_timeout = config.getint('poller', 'visibility_timeout')

    max_number_of_messages = 10
    if config.has_option('poller', 'max_number_of_messages'):
        max_number_of_messages = config.getint('poller', 'max_number_of_messages')

    if config.has_option('poller', 'wait_time'):
        wait_time = config.getint('poller', 'wait_time')
    else:
        wait_time = max_worker_uptime

    if wait_time > 20:
        wait_time = 20

    if forever:
        print("running forever ")

    # Init AWS SQS
    aws_key = config.get('aws', 'key')
    aws_secret = config.get('aws', 'secret')
    queue_name = config.get('aws', 'queue_name')
    queue_region = config.get('aws', 'region')
    sqs = boto3.client('sqs', region_name=queue_region)

    # Poll messages loop
    while True:
        message_count = 0
        try:
            response = sqs.receive_message(
                QueueUrl=queue_name,
                MaxNumberOfMessages=max_number_of_messages,
                WaitTimeSeconds=wait_time,
                AttributeNames=['All']
            )

            if 'Messages' in response:
                messages = response['Messages']
            else:
                messages = list()

            message_count = len(messages)
            logger.debug("Got %s message(s) this time." % message_count)

            for message in messages:
                message_handled = False

                try:
                    message_handled = handle_message(message)

                except Exception as e:
                    error_message = "Error {} while handling message:\n{}'".format(
                        str(e),
                        message.get('Body')
                    )
                    logger.exception(error_message)

                finally:
                    # This will hand the message back or delete if not handled after 5 attempts
                    if message_handled is False:
                        if int(message['Attributes']['ApproximateReceiveCount']) == max_receive_count:
                            error_message = "Deleting message after {} retries".format(
                                message['Attributes']['ApproximateReceiveCount']
                            )
                            logger.error(error_message)
                            message_handled = True

                    if message_handled is False:
                        sqs.change_message_visibility(
                            QueueUrl=queue_name,
                            ReceiptHandle=message['ReceiptHandle'],
                            VisibilityTimeout=visibility_timeout
                        )
                    else:
                        sqs.delete_message(
                            QueueUrl=queue_name,
                            ReceiptHandle=message['ReceiptHandle']
                        )

                if not forever and len(messages) == 0:
                    break

        except ClientError as e:
            error_message = "Client error {}".format(str(e))
            logger.error(error_message)
            time.sleep(30)

        except Exception as e:
            error_message = "Unexpected error {}. Will retry in 60 seconds".format(str(e))
            logger.exception(error_message)
            time.sleep(60)

        finally:
            if not forever:
                if (datetime.now() - start).total_seconds() > max_worker_uptime:
                    logger.debug("Worker uptime exceeded. exiting.")
                    break
                if message_count == 0:
                    logger.debug("Queue is empty. exiting.")
                    break


def handle_message(message):
    """
    Handle the message

    :param message:
    :return:
    """
    message_handled = False
    try:
        msg = json.loads(message.get('Body'))

        # this is the default handling - send to the logger
        logger.info(msg)
        message_handled = True

    except Exception as e:
        logger.error(str(e))

    return message_handled


if __name__ == '__main__':
    run()
