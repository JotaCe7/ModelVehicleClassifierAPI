# Model Vehicle Cassification API

## 1. Install

You can use `Docker` to install all the needed packages and libraries easily. Two Dockerfiles are provided for both CPU and GPU support.

- **CPU:**

```bash
$ docker build -t model_vehicle_class_jc --build-arg USER_ID=$(id -u) --build-arg GROUP_ID=$(id -g) -f docker/Dockerfile .
```

- **GPU:**

```bash
$ docker build -t model_vehicle_class_jc_gpu --build-arg USER_ID=$(id -u) --build-arg GROUP_ID=$(id -g) -f docker/Dockerfile_gpu .
```

### Run Docker

- **CPU:**

```bash
$ docker run --rm --net host -it -v "$(pwd)":/home/app/src --workdir /home/app/src model_vehicle_class_jc bash
```

- **GPU:**

```bash
$ docker run --rm --net host --gpus all -it -v "$(pwd)":/home/app/src --workdir /home/app/src model_vehicle_class_jc_gpu bash
$ docker run --rm --net host --gpus all -it -v "$(pwd)":/home/app/src --workdir /home/app/src sp_05 bash
```

python3 yolov5/detect_custome.py  --weights yolov5m.pt --source data/eu-car-only_autos/aaatest/ --name data/ --classes 2 --save-crop --nosave