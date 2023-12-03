import phiparser_test
import unittest

suite = unittest.TestLoader().loadTestsFromModule(phiparser_test);
unittest.TextTestRunner(verbosity=2).run(suite)

"""
if __name__ == '__main__':
    runner = unittest.TextTestRunner()
    runner.run(suite)
"""