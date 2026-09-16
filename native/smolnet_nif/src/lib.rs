use rustler::Atom;

mod atoms {
    rustler::atoms! {
        ok
    }
}

#[rustler::nif]
fn health() -> Atom {
    atoms::ok()
}

rustler::init!("Elixir.SmolNet.Native");
