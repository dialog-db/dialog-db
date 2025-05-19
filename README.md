# Staging

What if...

...your whole website was just a database?

...your database could embody more than just a website?

...you could read *and* write the data locally?

...it had better transfer and caching properties than actual websites?

...you could host the whole database in a plain-old browser tab?

## Contributing

Prerequisites:

- You must have the [Radicle CLI](https://radicle.xyz/download) installed (`rad` command)

After cloning this repository, run the setup script to configure the Git remotes and aliases

```bash
./scripts/setup.sh
```

This script will:

1. ✅ Check if the `rad` command is available
2. 👾 Set up Git aliases to work with radicle patches (eqivalent of pull requests)
3. ⛓️ Configure Git remotes for CI jobs.

If you want to be core contributor with push access, reach out with a request to get your radicle node to be added to the delegates. You will need to provide your node id by running following command

```sh
rad self --nid
```

[datalog]: https://en.wikipedia.org/wiki/Datalog
