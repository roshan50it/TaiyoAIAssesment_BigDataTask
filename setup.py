import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="TaiyoAIAssesment_BigDataTask",
    version="1.0.0",
    author="Roshan Kumar",
    author_email="roshan50it@gmail.com",
    description="Data engineering Role's Assessment",
    long_description=long_description,
    url="https://sample.assesment.com",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language:: Python :: 3",
        "Operating System :: OS Independent",
    ],
)
