import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="crystfel_stream_parser",
    version="24.02.03",
    author="Cong Wang",
    author_email="wangimagine@gmail.com",
    description="A crystfel stream parser.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/carbonscott/crystfel_stream_parser",
    keywords = ['crystfel_stream_parser'],
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    entry_points = {
        'console_scripts' : [
            ## 'peakdiff-visualizer-cxi=peakdiff.cxi.serve:main',
            ## 'peakdiff-visualizer-cxi=peakdiff.stream.serve:main',
        ],
    },
    python_requires='>=3.6',
    include_package_data=True,
)
