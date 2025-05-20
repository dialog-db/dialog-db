# Staging

What if...

...your whole website was just a database?

...your database could embody more than just a website?

...you could read *and* write the data locally?

...it had better transfer and caching properties than actual websites?

...you could host the whole database in a plain-old browser tab?

## Development

### Setup with Nix

This project uses Nix to provide a consistent development environment with all required dependencies.

1. Install [Nix](https://nixos.org/download.html) if you don't have it already
2. Enter the development environment:

```bash
nix develop
```

This automatically:
- Sets up all development dependencies (Rust, WebAssembly tools, etc.)
- Configures Git remotes and aliases for Radicle integration 
- Prepares your environment for building and testing


[datalog]:https://en.wikipedia.org/wiki/Datalog
[radicle]:https://radicle.xyz/
