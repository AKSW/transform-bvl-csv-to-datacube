Transform-bvl-csv-to-datacube
=============================

About
-----

Produce statistical data based on the building-database of the Behindertenverband Leipzig e.V. The purpose is to outline information about support for disabled people in public buildings at Leipzig.

Usage
-----

The run the process install [Jupyter](http://jupyter.org/index.html) and start a notebook with `jupyter notebook` in the same directory where your notebook files are.

After that open the corresponding notebook you want to use and run all cells in it by clicking `Cell -> Run Cells`.

### Update data

Replace the path to the provided CSV in the `querying` notebook and Run all cells. Afterwards run all cells from the `csv-2-datacube` notebook and new Datacube should be in the `dist` folder.
