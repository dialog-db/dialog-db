/*
let is_boss = Rule(
    [Variable::from("person")],
    Pattern::From(
        (
            Part::Variable(Variable::From("person")),
            Part::Literal(Literal::Attribute(Attribute::from_str("org/supervisorOf"))),
            Part::Variable(Variable::From("anyone"))
        )
    )
);

let is_parent_of = Rule(
    [Variable::from("parent"), [Variable::from("child")],
    Pattern::From(
        (
            Part::Variable(Variable::From("parent")),
            Part::Literal(Literal::Attribute(Attribute::from_str("relationship/parentOf"))),
            Part::Variable(Variable::From("child"))
        )
    )
);

let is_grandparent_of = Rule(
    [Variable::from("grandparent"), [Variable::from("child")],
    And(
        is_parent_of.query([Variable::from("grandparent"), Variable::from("parent")]),
        is_parent_of.query([Variable::from("parent"), Variable::from("child")]),
    )
);
*/

use crate::{Frame, Scope, Term, Variable};

use super::PullQuery;

pub struct Rule<const TERMS: usize, Q>
where
    Q: PullQuery,
{
    pub conclusion: [Variable; TERMS],
    pub body: Q,
}

impl<const TERMS: usize, Q> Rule<TERMS, Q>
where
    Q: PullQuery,
{
    pub fn query(&self, terms: [Term; TERMS]) -> RuleQuery<TERMS, Q> {
        let scope = Scope::new();
        let conclusion = self
            .conclusion
            .iter()
            .map(|variable| variable.scope(&scope))
            .collect::<Vec<Variable>>()
            .try_into()
            .unwrap();
        let query = self.body.scope(&scope);

        RuleQuery {
            query,
            conclusion,
            terms,
        }
    }
}

pub struct RuleQuery<const TERMS: usize, Q>
where
    Q: PullQuery,
{
    query: Q,
    conclusion: [Variable; TERMS],
    terms: [Term; TERMS],
}

impl<const TERMS: usize, Q> RuleQuery<TERMS, Q>
where
    Q: PullQuery,
{
    pub fn unify(&self, frame: &Frame) -> (Frame, Q) {
        let mut frame = frame.clone();
        for (index, term) in self.terms.iter().enumerate() {
            match term {
                Term::Constant(literal) => {

                    // if let Some(kind) = self.query.variable_kind(&self.conclusion[index]) {
                    // }
                    // frame = frame.assign(self.conclusion[index], Varia);
                }
                Term::Variable(variable) => todo!(),
            }
        }
        todo!();
    }
}
