import os
import sys

TEST_DIRECTORY = os.path.dirname(__file__)

SRC_DIRECTORY = os.path.join(os.path.dirname(TEST_DIRECTORY), "ostrich-egg")

sys.path.insert(0, SRC_DIRECTORY)
sys.path.insert(1, TEST_DIRECTORY)
