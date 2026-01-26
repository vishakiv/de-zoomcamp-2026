# Week 1: Introduction to Docker and Terraform, SQL refresher


### Contents


### Homework

Q1. Understanding Docker Images
Run docker with the python:3.13 image. Use an entrypoint bash to interact with the container. What's the version of pip in the image?

Answer: 25.3 
Shell command
```bash
docker run -it --rm --entrypoint=bash python:3.13
pip --version
```
