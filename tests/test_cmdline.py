from unittest import TestCase, main
from unittest.mock import patch
from batch_client.parser import create_parser, parse_cmdline
from .test_base import MockDevice


class CommandLineTestCase(TestCase):
    @classmethod
    def setUpClass(cls):
        parser = create_parser()
        cls.parser = parser


class TestBatchCommandLine(CommandLineTestCase):
    def test_invalid_combinations(self):
        with patch('sys.stderr', new=MockDevice()):
            with self.assertRaises(SystemExit):
                parse_cmdline(['-input_list', 'blah.txt', '-m', 'DAEMON'])

    def test_invalid_choice(self):
        with patch('sys.stderr', new=MockDevice()):
            with self.assertRaises(SystemExit):
                self.parser.parse_args(['-m', 'daemon'])

    def test_invalid_values(self):
        with patch('sys.stderr', new=MockDevice()):
            with self.assertRaises(SystemExit):
                self.parser.parse_args(['-nbest', '0'])

    def test_default_values(self):
        args = parse_cmdline([])
        self.assertEqual(args.configuration_file, "/usr/local/batch/input/config.yaml")
        self.assertEqual(args.output_folder, "/usr/local/batch/output")
        self.assertEqual(args.input_folder, "/usr/local/batch/input")
        self.assertEqual(args.run_mode, "ONESHOT")
        self.assertEqual(args.diarization_mode, "None")
        self.assertEqual(args.nbest, 1)


if __name__ == '__main__':
    main()
