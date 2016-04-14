"""
Example unit tests for Level2_Pipeline package
"""
import unittest
import desc.level2_pipeline

class Level2_PipelineTestCase(unittest.TestCase):
    def setUp(self):
        self.message = 'Hello, world'
        
    def tearDown(self):
        pass

    def test_run(self):
        foo = desc.level2_pipeline.Level2_Pipeline(self.message)
        self.assertEquals(foo.run(), self.message)

    def test_failure(self):
        self.assertRaises(TypeError, desc.level2_pipeline.Level2_Pipeline)
        foo = desc.level2_pipeline.Level2_Pipeline(self.message)
        self.assertRaises(RuntimeError, foo.run, True)

if __name__ == '__main__':
    unittest.main()
