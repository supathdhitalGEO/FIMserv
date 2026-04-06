## Contributing to *```FIMserv```*
Thank you for considering contributing to this project! Whether it's fixing a bug, improving documentation, or adding a new feature, we welcome contributions.
There is a minimal set of standards we would ask you to consider to speed up the review process.

### How to Contribute?

1. **Fork the repository**
   - If you have not already done so, create a fork of the `FIMserv` repo (main branch) and make changes to this copy.

2. **Set up the development environment**
   - Make sure development packages for RiverJoin are installed. This can be done by flagging dev packages during install:
     ```bash
     git clone https://github.com/sdmlab/FIMserv.git
     cd FIMserv
     pip install uv
     uv venv
     source .venv/bin/activate        # Windows: .venv\Scripts\activate
     uv pip install -e .
     uv pip install -e ".[dev]"
     ```

3. **Lint & test your code**
   - Once your changes are complete, run the following in your Python environment:
     ```bash
     cd FIMserv
     pytest tests/
     black .
     git add .
     git commit -m "your commit message"
     ```
   - `black` will auto-format your code to a consistent style before committing.
   - If `pytest` does not work, try `python -m pytest`.
   - If `black` or `pytest` report any errors, please try to correct these if possible. Otherwise, commit with `--no-verify` to proceed and we can help in the next step.

4. **Make a pull request (PR)**
   - When you are ready, make a PR of your fork to the `FIMserv` repository main branch.
   - In the PR description, include enough detail so that we can understand the changes made and the rationale if necessary.
   - If the `FIMserv` main branch has new commits not included in your forked version, we would ask you to merge these new changes into your fork before we accept the PR. We can assist with this if necessary.

### Reporting Bugs & Suggesting Features

- For bugs, please [open an issue](https://github.com/sdmlua/FIMserv/issues) with steps to reproduce, expected vs. actual behavior, and any relevant error messages.
- For feature requests, describe the problem you are trying to solve and your proposed approach. Referencing related datasets, tools, or papers is always helpful.

### Contact

For questions, reach out to <a href="https://geography.ua.edu/people/sagy-cohen/" target="_blank">Dr. Sagy Cohen</a>
 (sagy.cohen@ua.edu), Supath Dhital (sdhital@crimson.ua.edu), Dr. Anupal Baruah,(abaruah@ua.edu)
