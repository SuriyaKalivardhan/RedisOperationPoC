from TestModule import *
import itertools
import threading

def main():
    for i in range(500):
        TestModule()
    # for i in itertools.count():
    #     print(i)
    threading.Event().wait()

if __name__ == "__main__":
    main()