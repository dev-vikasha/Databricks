
import time 
import multiprocessing

def calc_square (arr):
    for num in arr:
        time.sleep(5)
        print("Square :" + str(num*num))

def calc_cube (arr):
    for num in arr:
        time.sleep(5)
        print("Cube :" + str(num*num*num))

arr = [2,4,6,8]

if __name__ == "__main__":
    p1 = multiprocessing.Process(target=calc_cube, args=(arr,))
    p2 = multiprocessing.Process(target=calc_square, args=(arr,))

    t = time.time()
    p1.start()
    p2.start()

    p1.join()
    p2.join()

    print("All Process has been Completed ")