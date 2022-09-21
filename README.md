# packageName

## Setup

Choose a package name and replace the template name `packageName` in all files:
```bash
./quick-start.sh MyPackageName
rm quick-start.sh  # you can delete this file afterwards
```


## Upload to pypi
```bash
pip install --upgrade pip
pip install wheel twine
```

Place your [pypi token](https://pypi.org/manage/account/token/) in `$HOME/.pypirc`:

```
[pypi]
  username = __token__
  password = # either a user-scoped token or a project-scoped token you want to set as the default
```

To upload to pypi,
```
make
```

## Install
```bash
pip install packageName
pip install -e <path-to-repo>/packageName
pip install git+ssh://git@github.com/<path-to-repo>.git
```
