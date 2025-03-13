import os
import sys

TEST_DIRECTORY = os.path.dirname(__file__)

SRC_DIRECTORY = os.path.join(os.path.dirname(TEST_DIRECTORY), "ostrich_egg")
ROOT_DIR = os.path.dirname(SRC_DIRECTORY)

sys.path.insert(0, ROOT_DIR)
sys.path.insert(1, SRC_DIRECTORY)
sys.path.insert(2, TEST_DIRECTORY)
