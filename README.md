Transform-bvl-csv-to-datacube
=============================

About
-----

Produce statistical data based on the building-database of the Behindertenverband Leipzig e.V. The purpose is to outline information about support for disabled people in public buildings at Leipzig.

Usage
-----

To run the process install [Jupyter](http://jupyter.org/index.html) and start a notebook with `jupyter notebook` in the same directory where your notebook files are.

After that open the corresponding notebook you want to use and run all cells in it by clicking `Cell -> Run Cells`.

### Update data

Replace the path to the provided CSV in the `querying` notebook and Run all cells. Afterwards run all cells from the `csv-2-datacube` notebook and new Datacube should be in the `dist` folder.

### Docker

Also you could run the transformation in a Docker container. To build the Docker image run the following command within the repository folder:

```
docker build -t bvl docker/
```

To start the process define a path to a folder on your host system which contains a `source.csv` file and run:

```
 docker run -v /path/to/folder:/shared bvl
```

The results will be placed in the same folder.
