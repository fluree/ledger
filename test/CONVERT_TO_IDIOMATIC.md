# Converting our test suite to idiomatic Clojure tests

## Goals

- Make the tests runnable individually, together with other tests, and in any
  order
- Make the tests easily runnable from the CLI, in CI, and at the REPL
- Make / keep the test suite performant (i.e. improve it where you can but at
  the very least do no harm)
- Make / keep the test suite reliable (same idea: first, do no harm. second, do 
  good.)
- Make the test suite more familiar to other Clojure programmers by following
  community conventions & idioms

## Process

> It's generally going to be easiest to convert a namespace at a time.
However, if that's a bigger lift than you have time for, it's also great to
move some tests into a new idiomatic ns and keep the old one around with the
unconverted ones.

1. Rename & move the ns so that its name ends in `-test`. This allows the
   default test runner to automatically find and run the tests. Or make a new
   ns with the idiomatic name if you aren't converting the entire ns.
2. Replace the :once fixture using `test-helpers/test-system-deprecated`
   with a :once fixture using `test-helpers/test-system`. This just starts
   the in-memory ledger but does not create any ledgers in it.
3. Use `test-helpers/rand-ledger` for tests that need to create one. Use it in
   each individual test so they get their own unique ledger sandbox and can be
   run individually, with other tests, and in any order.
4. Move any non-clj files it uses (e.g. schema / data .edn files) into the
   `test-resources` hierarchy.
    - Follow the existing schema / data & file name
   conventions.
    - Delete any old directories that are now empty.
    - Update any other usages of that file to point to the new path (use
      `(clojure.java.io/resource "relative/path/in/test-resources")`)
5. Use `test-helpers/transact-resource` (and its convenience partials
   `transact-schema` & `transact-data`) in each test to pull in schema & data
   from the `test-resources` hierarchy.
6. Lowercase & kebab-case any lingering mixed-case / CamelCase dir / filenames.
7. For unit tests, name the test `[fn-it-is-testing]-test`.
8. Remove all deftest forms that just run other tests. They are no longer 
   needed and will just run tests more than once unnecessarily.
9. Go over the code to check for simple & important fixes. This is a balancing
   act. The goal isn't to get bogged down in a big refactor (there's enough to
   do here as it is), but if you see things like the following non-exhaustive
   list of examples, fix them:
     - Assertions missing their `(is ...)` wrapper
     - Assertions that are always true (e.g. `(is (map ...))` instead of
        `(is (every? ...))`)
         - For this one especially you'll often find that the assertion doesn't
           hold after you fix it. Use your best judgment, ask others, or just
           comment it out w/ a "TODO: Fix me" kind of comment.
     - `testing` forms that don't wrap their test code
        (i.e. `(testing "stuff") (test code)` instead of
         `(testing "stuff" (test code))`)
     - Comments in addition to or instead of `testing` forms; delete these and
       add a `testing` form where necessary
     - Redundant fn wrappers around things that can behave as fns on their own
       (e.g. sets and keywords)
     - But overall, use your best judgment. If something seems simple but turns
       into a rabbit hole, leave it for another time.
10. Run the test suite (`make test`) and make sure your new tests are running
    and passing.