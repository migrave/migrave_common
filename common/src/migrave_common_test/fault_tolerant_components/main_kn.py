#!/usr/bin/python3
import argparse
from migrave_common_test.fault_tolerant_components.components.src.knowledge_base.run import run as run_knowledge_base
import signal
import sys


def interrupt_handler(signum, frame):
    sys.exit(-2)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Fault tolerant src status',
                                     epilog='EXAMPLE: python3 main_kn.py')

    signal.signal(signal.SIGINT, interrupt_handler)
    run_knowledge_base()
