
# Development installation

Just use (made env.yml file before)

```
conda env create -f env.yml
```

then run it  by 

```
conda activate stem_env
```

If there's need to  install in editable mode:

```
cd ./stem_framework
pip install -e .
```

# Documentation by Sphinx

```
python setup.py build_sphinx
```



