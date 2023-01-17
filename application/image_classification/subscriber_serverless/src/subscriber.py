"""\
This is a subscriber, receiving images through HTTP as serverless and
processing them using image classification from TFLite.
"""

import io
import os
import time
from PIL import Image
import numpy as np
import tflite_runtime.interpreter as tflite

CPU_THREADS = int(os.environ["CPU_THREADS"])


def handle(req):
    """A Multiprocessing thread
    Receive images from a queue, and perform image classification on it

    Args:
        req (str): Request body as string
    """
    t_now = time.time_ns()
    print("Start\n")

    # Load the labels
    with open("labels.txt", "r", encoding="utf-8") as f:
        labels = [line.strip() for line in f.readlines()]

    # Load the model
    threads = max(1, CPU_THREADS)
    interpreter = tflite.Interpreter(model_path="model.tflite", num_threads=threads)
    interpreter.allocate_tensors()

    # Get model input details and resize image
    input_details = interpreter.get_input_details()
    floating_model = input_details[0]["dtype"] == np.float32

    iw = input_details[0]["shape"][2]
    ih = input_details[0]["shape"][1]

    print("Preparations finished")

    # Get the input data
    data = bytearray(req)

    # Get timestamp to calculate latency.
    # We prepended 0's to the time to make it a fixed length
    t_bytes = data[-20:]
    t_old = int(t_bytes.decode("utf-8"))
    print("Latency (ns): %s\n" % (str(t_now - t_old)), end="")

    # Get data to process
    data = data[:-20]
    image = Image.open(io.BytesIO(data))
    image = image.resize((iw, ih)).convert(mode="RGB")

    input_data = np.expand_dims(image, axis=0)

    if floating_model:
        input_data = (np.float32(input_data) - 127.5) / 127.5

    interpreter.set_tensor(input_details[0]["index"], input_data)

    interpreter.invoke()

    output_details = interpreter.get_output_details()
    output_data = interpreter.get_tensor(output_details[0]["index"])
    results = np.squeeze(output_data)

    top_k = results.argsort()[-5:][::-1]
    for i in top_k:
        if floating_model:
            print("\t{:08.6f} - {}\n".format(float(results[i]), labels[i]), end="")
        else:
            print(
                "\t{:08.6f} - {}\n".format(float(results[i] / 255.0), labels[i]),
                end="",
            )

    sec_frame = time.time_ns() - t_now
    print("Processing (ns): %i\n" % (sec_frame), end="")

    return t_bytes
