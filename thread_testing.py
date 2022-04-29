### TO DO
# Handle exceptions within ThreadPoolExecutor - futures have an .exception() method which can be useful

import argparse
import json
from re import L, T

from henry.commands import analyze, pulse, vacuum
from henry.modules import fetcher
from henry.cli import setup_cli
from datetime import datetime as dt


def time(fn):
    def inner(*args, **kwargs):
        s = dt.now()
        r = fn(*args, **kwargs)
        e = dt.now()
        t = (e - s).total_seconds()
        print(
            f'{fn.__name__} completed in {t:.2f} seconds with {args[0]} thread(s)')
        return r
    return inner


def custom_init(parser: argparse.ArgumentParser, threads, quiet=False, command_overrides=None):
    args = dict(vars(parser.parse_args()))
    args['threads'] = threads
    if quiet:
        args['quiet'] = True
    if command_overrides:
        args['command'] = command_overrides[0]
        if command_overrides[1]:
            args['subcommand'] = command_overrides[1]
    print(args)
    return fetcher.Input(**args)


@time
def thread_test(n):
    parser = setup_cli()
    user_input = custom_init(parser, n)
    if user_input.command == "pulse":
        pulse.Pulse.run(user_input)
    elif user_input.command == "analyze":
        analyze.Analyze.run(user_input)
    elif user_input.command == "vacuum":
        vacuum.Vacuum.run(user_input)
    else:
        parser.error()


def test_threading(vals):
    for v in vals:
        thread_test(v)


def test_all(threads, num_iters=8):
    commands = [('pulse', 'None'), ('analyze', 'projects'), ('analyze', 'models'),
                ('analyze', 'explores'), ('vacuum', 'models'), ('vacuum', 'explores')]
    parser = setup_cli()
    results = {}
    n = 0
    total = len(threads) * num_iters * len(commands)
    for _ in range(num_iters):
        for thread in threads:
            results[thread] = {}
            for command_pair in commands:
                cp_string = '-'.join(command_pair)
                results[thread][cp_string] = {'runs': [], 'avg': None}
                user_input = custom_init(parser, thread, True, command_pair)
                try:
                    n += 1
                    s = dt.now()
                    if command_pair[0] == "pulse":
                        pulse.Pulse.run(user_input)
                    elif command_pair[0] == "analyze":
                        analyze.Analyze.run(user_input)
                    elif command_pair[0] == "vacuum":
                        vacuum.Vacuum.run(user_input)
                    else:
                        parser.error()
                    e = dt.now()
                    t = (e - s).total_seconds()
                    results[thread][cp_string]['runs'].append(round(t, 2))
                    print(
                        f'Test {n:>3}/{total}: threads: {thread:<2} - time: {t:<5.2f}s')
                except Exception as e:
                    print(
                        f"Test {n:>3}/{total}: threads: {thread:<2} - ERROR:\n\t{str(e)}")
                    pass
    for k in results.keys():
        for k2 in k.keys():
            if len(results[k][k2]['runs']) > 0:
                results[k]['avg'] = round(
                    sum(results[k]['runs']) / len(results[k]['runs']), 2)
    with open('thread_testing_results.json', 'w') as f:
        f.write(json.dumps(results))
    print(results)
    return results


def test_iter(ns, num_iters=8):
    parser = setup_cli()
    results = {}
    n = 0
    total = len(ns) * num_iters
    for _ in range(num_iters):
        for threads in ns:
            results[threads] = {'runs': [], 'avg': None}
            user_input = custom_init(parser, threads, True)
            try:
                n += 1
                s = dt.now()
                if user_input.command == "pulse":
                    pulse.Pulse.run(user_input)
                elif user_input.command == "analyze":
                    analyze.Analyze.run(user_input)
                elif user_input.command == "vacuum":
                    vacuum.Vacuum.run(user_input)
                else:
                    parser.error()
                e = dt.now()
                t = (e - s).total_seconds()
                results[threads]['runs'].append(round(t, 2))
                print(
                    f'Test {n:>3}/{total}: threads: {threads:<2} - time: {t:<5.2f}s')
            except Exception as e:
                print(
                    f"Test {n:>3}/{total}: threads: {threads:<2} - ERROR:\n\t{str(e)}")
                pass
    for k in results.keys():
        if len(results[k]['runs']) > 0:
            results[k]['avg'] = round(
                sum(results[k]['runs']) / len(results[k]['runs']), 2)
    with open('thread_testing_results.json', 'w') as f:
        f.write(json.dumps(results))
    print(results)
    return results


if __name__ == "__main__":
    test_all([8, 1], 4)
