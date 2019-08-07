# How to contribute

## Testing

## Submitting changes

Please send a [GitHub Pull Request to stock-study](https://github.com/Soncrates/stock-study/pull/new/master) with a clear list of what you've done (read more about [pull requests](http://help.github.com/pull-requests/)). When you send a pull request.  Please follow coding conventions (below) and make sure all of your commits are atomic (one feature per commit).

Always write a clear log message for your commits. One-line messages are fine for small changes, but bigger changes should look like this:

    $ git commit -m "A brief summary of the commit
    > 
    > A paragraph describing what changed and its impact."

## Coding conventions

  * Indent using spaces (no tabs)
  * Any code prefixed with "lib" must have a main to demonstrate how to use your code
  * Unit test code to reveal edge cases is always appreciated
  * Your code must be readable, and use descriptive nouns for variable and class names and include a verb in method and function names.
  * I am a big fan of lambda and passing **kwargs instead of individual parameters, I understand if that makes your eyeballs bleed, but I am not changing it.
Thanks,
