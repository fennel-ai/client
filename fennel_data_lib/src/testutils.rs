use itertools::Itertools;
use crate::schema::DSSchema;
use crate::schema::Field;
use crate::types::Type;
use crate::Value;
use crate::{Expr, BinOp, UnOp};
use crate::expr::MathFn;

pub struct DSSchemaBuilder {
    keys: Option<Vec<Field>>,
    values: Option<Vec<Field>>,
    tsfield: Option<String>,
    erase_keys: Option<Vec<String>>,
}

impl DSSchemaBuilder {
    pub fn new() -> Self {
        Self {
            keys: None,
            values: None,
            tsfield: None,
            erase_keys: None,
        }
    }

    pub fn keys(mut self, keys: &[(&str, Type)]) -> Self {
        let fields = keys
            .iter()
            .map(|(name, typ)| Field::new(name.to_string(), typ.clone()))
            .collect();
        self.keys = Some(fields);
        self
    }

    pub fn values(mut self, values: &[(&str, Type)]) -> Self {
        let fields = values
            .iter()
            .map(|(name, typ)| Field::new(name.to_string(), typ.clone()))
            .collect();
        self.values = Some(fields);
        self
    }

    pub fn tsfield(mut self, tsfield: &str) -> Self {
        self.tsfield = Some(tsfield.to_string());
        self
    }

    pub fn erase_keys(mut self, erase_keys: &[&str]) -> Self {
        self.erase_keys = Some(erase_keys.iter().map(|s| s.to_string()).collect_vec());
        self
    }

    pub fn build(self) -> DSSchema {
        let keyschema = self
            .keys
            .unwrap_or(vec![Field::new("key".to_string(), Type::Int)]);
        let valschema = self
            .values
            .unwrap_or(vec![Field::new("val".to_string(), Type::Int)]);
        let erase_keys = self.erase_keys.unwrap_or(vec![]);
        DSSchema::new_with_erase_keys(
            keyschema,
            self.tsfield.unwrap_or("timestamp".to_string()),
            valschema,
            erase_keys,
        )
        .unwrap()
    }
}

pub fn binary_expr() -> Expr {
    Expr::Binary {
        op: BinOp::Add,
        left: Box::new(Expr::Lit {
            value: Value::Int(12343) 
        }),
        right: Box::new(Expr::Lit {
            value: Value::Int(332)
        }),
    }
}

pub fn unary_expr() -> Expr {
    Expr::Unary {
        op: UnOp::Neg,
        expr: Box::new(Expr::Lit {
            value: Value::Int(12343)
        }),
    }
}

pub fn math_expr() -> Expr {
    Expr::MathFn {
        func: MathFn::Abs,
        expr: Box::new(Expr::Lit {
            value: Value::Int(-12343)
        }),
    }
}

pub fn whenthen_expr() -> Expr {
    Expr::Case {
        when_thens: vec![
            (Expr::Binary {
                op: BinOp::Eq,
                left: Box::new(Expr::Ref { name: "a".to_string() }),
                right: Box::new(Expr::Lit { value: Value::Int(123) }),
            },
            Expr::Lit { value: Value::Int(1) }),
            (
                Expr::Binary {
                    op: BinOp::Eq,
                    left: Box::new(Expr::Ref { name: "a".to_string() }),
                    right: Box::new(Expr::Lit { value: Value::Int(124) }),
                },
                Expr::Lit { value: Value::Int(2) },
            ),
        ],
        otherwise: Some(Box::new(Expr::Lit { value: Value::Int(0) })),
    }
} 