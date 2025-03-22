use crate::{Scope, Term, Variable, XQueryError};

use super::PullQuery;

pub struct Rule<const ARITY: usize, Q>
where
    Q: PullQuery,
{
    pub conclusion: [Variable; ARITY],
    pub body: Q,
}

impl<const ARITY: usize, Q> Rule<ARITY, Q>
where
    Q: PullQuery,
{
    pub fn query(&self, terms: [Term; ARITY]) -> Result<Q, XQueryError> {
        let scope = Scope::new();

        // Scope variables in the conclusion
        let conclusion: [Variable; ARITY] = self
            .conclusion
            .iter()
            .map(|variable| variable.scope(&scope))
            .collect::<Vec<Variable>>()
            .try_into()
            .unwrap();

        // Scope variables in the body
        let mut query = self.body.scope(&scope);

        // Unify the conclusion and body with the incoming terms
        for (position, alternate) in terms.iter().enumerate() {
            query = query.substitute(&conclusion[position], alternate)?;
        }

        Ok(query)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        Frame, Pattern, PrimaryKey, TripleStore, Value, Variable, make_store,
        pull::{And, PullQuery, Rule},
    };
    use anyhow::Result;
    use futures_util::{TryStreamExt, stream};

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_can_define_a_rule_and_use_it_in_a_query() -> Result<()> {
        let (store, _) = make_store().await?;

        let item_name = Rule {
            conclusion: [Variable::from("id"), Variable::from("name")],
            body: And(
                Pattern::try_from((
                    Variable::from("entity"),
                    Value::Symbol("item/id".into()),
                    Variable::from("id"),
                ))?,
                Pattern::try_from((
                    Variable::from("entity"),
                    Value::Symbol("item/name".into()),
                    Variable::from("name"),
                ))?,
            ),
        };

        let test_query =
            item_name.query([Value::UnsignedInt(0).into(), Variable::from("name").into()])?;

        println!("{:#?}", test_query);

        for i in 0..8u128 {
            let stream = item_name
                .query([Value::UnsignedInt(i).into(), Variable::from("name").into()])?
                .stream(store.clone(), stream::once(async { Ok(Frame::default()) }));

            tokio::pin!(stream);

            let frame = stream.try_next().await?.expect("There is an output frame");
            let key = PrimaryKey::from(
                frame
                    .read(&Variable::from("name"))
                    .expect("A value is assigned to the name variable"),
            );
            let (_, _, value) = store.read(&key).await?.expect("A datum exists for the key");

            assert_eq!(value, Value::String(format!("name{i}")));
        }
        Ok(())
    }
}
