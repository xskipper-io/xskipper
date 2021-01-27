<!--
 -- Copyright 2021 IBM Corp.
 -- SPDX-License-Identifier: Apache-2.0
 -->

# API Doc Generation

The following contains instructions on how to generate the Scala and Python docs for Xskipper.

## Xskipper Python Docs

Xskipper Python docs are generated using [Sphinx](https://www.sphinx-doc.org/en/master/)

### Installation

```
pip install -U Sphinx
pip install mock
```

### Local Changes

The doc configs are location in `python/doc`.

Make the needed changes to the python files and then create the HTML files using:

```
cd python/doc
make html
```

The results will be located under `_build/html`

!!! note
    By default the version that will be used is the version that is written in`version.sbt`
    If you would like to change the version change it in the config file `conf.py`

### Publishing

To update the docs copy `_build/html` to `site/docs/api/pythondoc/<version_number>`.  
For example:

```
cp -r _build/html ../../site/docs/api/pythondoc/1.2.0
```

Then update the [index](/site/docs/api/pythondoc/index.html) file to point to the new API documentation.

## Xskipper Scala Docs

Xskipper Scala docs are generated using [sbt doc](https://www.scala-sbt.org/1.x/docs/Howto-Scaladoc.html)

### Generate the docs

Run the following:

```
sbt clean compile package
sbt doc
```

### Publishing

To update the docs copy `../../../../target/scala-2.12/api/` to `../scaladoc/1.2.0/` (assuming you shell is pointed at this folder).  
For example:

```
cp -r ../../../../target/scala-2.12/api/ ../scaladoc/1.2.0/
```

Then update the [index](/site/docs/api/scaladoc/index.html) file to point to the new API documentation.