"""
Created on 1 May 2020

@author: shree
"""


def load_benchmark():
    try:
        with open('../../data/restaurant_pair.csv', 'r') as f:
            benchmark = f.read().splitlines()
        if benchmark is None:
            print("no file.")
        return benchmark
        # print(benchmark)


    except:
        print("Error occurred. Check if file exists")


#


def calc_measure(results):
    benchmark = load_benchmark()
    if len(results) == 0:
        print('Precision = 0, Recall = 0, Fmeasure = 0')
        return
    count = 0
    for pair in results:

        if pair in benchmark:
            count = count + 1
    if count == 0:
        print('Precision=0, Recall=0, Fmeasure=0')
        return
    precision = count / len(results)
    recall = count / len(benchmark)
    f_measure = 2 * precision * recall / (precision + recall)

    tp = count
    fp = len(results) - count
    fn = len(benchmark) - count

    tpr = tp / (tp + fn)
    fnr = fn / (fn + tp)

    print("Student_id: s4761003")
    print("True Positive: " + str(count))
    print("False Positive: " + str(len(results) - count))
    print("False Negative: " + str(len(benchmark) - count))
    print("True Negative: Cannot be calculated")

    print("True Positive Rate: " + str(tpr))
    print("False Negative Rate: " + str(fnr))
    print("True Negative Rate: Cannot be calculated as the value of True Negative is not known")
    print("False Positive Rate: Cannot be calculated as the value of True Negative is not known")

    print("Precision=", precision, ", Recall=", recall, ", Fmeasure=", f_measure)

    return precision, recall, f_measure

# load_benchmark()
