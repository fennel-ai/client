# Docs
This repo houses all of the content that powers our documentation.

## Get Started
To get up and running contributing to the documentation, take the following step:
1. Clone this repo
2. Rename `.env.example` to `.env` and fill out the values.
3. Run in your terminal `make up` from the root 
	- This will pull in the Docs UI repo from Github, and run it on `localhost:3001/docs`
4. Edit the markdown and python files in this repo, and get hot-reloading showing the latest changes on `localhost`
5. Commit changes once you're ready.
	- Upon commit, the python files will run through the test suite and block any broken examples from going live in the documentation.

> When new updates are made to the UI, you may need to run `make build` before `make up` in order to force Docker to refetch the latest changes and rebuild the image.

## `./examples`
The example directory holds Python test files. Anywhere in these files you can wrap any number of lines between `# docsnip` comments 
**e.g.** `example.py`:
```python
from fennel import * 

# docsnip my_snippet
@dataset
class UserInfoDataset:
	name: str
	email: str
	id: str
	age: int
# /docsnip

	def my_pipeline():
		# todo
		return False
```

Now, in any of our markdown files you can write:
```html
<pre snippet="example#my_snippet" />
```

Here we are selecting the `examples/example.py` file, and the `my_snippet` snippet within. You can think of this as the following:

`<path_to_file>#<snippet_id>`
> The slug of the file (the path to the file within `./examples` without `.py`, followed by the snippet id.)


## `./pages`

This directory includes of all the source markdown files for our documentation. Each markdown file in here is rendered in React as JSX, allowing us to very easily extend the markdown syntax with our own custom React components.

## `config.yml`
The `config.yml` file is used to set global configuration options for the docs. Currently we only use this to define the navigation sidebar. Section and their Pages within the config file are added to the left-hand navigation of the documentation UI.

Any pages that are _not_ in the file are still generated in dev and production (if they are not a `draft`) and can be navigated/linked to, but won't appear in the main navigation.

The `version` field gives us a way to easily pick out the version tag for this branch of the documentation from the UI side.