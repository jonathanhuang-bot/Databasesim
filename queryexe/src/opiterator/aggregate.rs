use super::{OpIterator, TupleIterator};
use common::{AggOp, Attribute, CrustyError, DataType, Field, TableSchema, Tuple};
use std::cmp::{max, min};
use std::collections::HashMap;
use std::sync::Arc;

/// Contains the index of the field to aggregate and the operator to apply to the column of each group. (You can add any other fields that you think are neccessary)
#[derive(Clone)]
pub struct AggregateField {
    /// Index of field being aggregated.
    pub field: usize,
    /// Agregate operation to aggregate the column with.
    pub op: AggOp,
}
impl AggregateField {
    pub fn clone(&self) -> Self {
        match self.op {
            AggOp::Avg => AggregateField {
                field: self.field,
                op: AggOp::Avg,
            },
            AggOp::Max => AggregateField {
                field: self.field,
                op: AggOp::Max,
            },
            AggOp::Min => AggregateField {
                field: self.field,
                op: AggOp::Min,
            },
            AggOp::Count => AggregateField {
                field: self.field,
                op: AggOp::Count,
            },
            AggOp::Sum => AggregateField {
                field: self.field,
                op: AggOp::Sum,
            },
        }
    }
}

/// Computes an aggregation function over multiple columns and grouped by multiple fields. (You can add any other fields that you think are neccessary)
struct Aggregator {
    /// Aggregated fields.
    agg_fields: Vec<AggregateField>,
    /// Group by fields
    groupby_fields: Vec<usize>,
    /// Schema of the output.
    schema: TableSchema,
    groups: HashMap<Vec<Field>, (Vec<Field>, i32)>,
    //count: HashMap<Vec<Field>, Field>
}

impl Aggregator {
    /// Aggregator constructor.
    ///
    /// # Arguments
    ///
    /// * `agg_fields` - List of `AggregateField`s to aggregate over. `AggregateField`s contains the aggregation function and the field to aggregate over.
    /// * `groupby_fields` - Indices of the fields to groupby over.
    /// * `schema` - TableSchema of the form [groupby_field attributes ..., agg_field attributes ...]).
    fn new(
        agg_fields: Vec<AggregateField>,
        groupby_fields: Vec<usize>,
        schema: &TableSchema,
    ) -> Self {
        Aggregator {
            agg_fields,
            groupby_fields,
            schema: schema.clone(),
            groups: HashMap::new(),
        }
    }

    /// Handles the creation of groups for aggregation.
    ///
    /// If a group exists, then merge the tuple into the group's accumulated value.
    /// Otherwise, create a new group aggregate result.
    ///
    /// # Arguments
    ///
    /// * `tuple` - Tuple to add to a group.
    pub fn merge_tuple_into_group(&mut self, tuple: &Tuple) {
        
        let groupbyfields = &self.groupby_fields;
        let aggfields = &self.agg_fields;
        let mut key = Vec::new();
        //determine key for group
        for i in groupbyfields {
            let keyfiel = tuple.get_field(*i).unwrap();
            let keyfield = keyfiel.clone();
            key.push(keyfield);
        }
        if self.groups.contains_key(&key) {
            let (prevagg, count) = self.groups.get_mut(&key).unwrap();
            let mut newagg = Vec::new();
            let mut index = 0;
            for p in aggfields {
                if tuple.get_field(p.field).unwrap().is_int() {
                    let new = match p.op {
                        common::AggOp::Avg => {
                            prevagg[index].unwrap_int_field()
                                + tuple.get_field(p.field).unwrap().unwrap_int_field()
                        }
                        common::AggOp::Count => prevagg[index].unwrap_int_field() + 1,
                        common::AggOp::Max => {
                            if tuple.get_field(p.field).unwrap().unwrap_int_field()
                                > prevagg[index].unwrap_int_field()
                            {
                                tuple.get_field(p.field).unwrap().unwrap_int_field()
                            } else {
                                prevagg[index].unwrap_int_field()
                            }
                        }
                        common::AggOp::Min => {
                            if tuple.get_field(p.field).unwrap().unwrap_int_field()
                                < prevagg[index].unwrap_int_field()
                            {
                                tuple.get_field(p.field).unwrap().unwrap_int_field()
                            } else {
                                prevagg[index].unwrap_int_field()
                            }
                        }
                        common::AggOp::Sum => {
                            prevagg[index].unwrap_int_field()
                                + tuple.get_field(p.field).unwrap().unwrap_int_field()
                        }
                    };
                    let n = Field::IntField(new);
                    newagg.push(n);
                    index += 1;
                } else {
                    if p.op.max_min() {
                        let new = match p.op {
                            common::AggOp::Max => {
                                if tuple.get_field(p.field).unwrap().unwrap_string_field()
                                    > prevagg[index].unwrap_string_field()
                                {
                                    tuple.get_field(p.field).unwrap().unwrap_string_field()
                                } else {
                                    prevagg[index].unwrap_string_field()
                                }
                            }
                            common::AggOp::Min => {
                                if tuple.get_field(p.field).unwrap().unwrap_string_field()
                                    < prevagg[index].unwrap_string_field()
                                {
                                    tuple.get_field(p.field).unwrap().unwrap_string_field()
                                } else {
                                    prevagg[index].unwrap_string_field()
                                }
                            }
                            _ => panic!(""),
                        };
                        let n = Field::StringField(new.to_string());
                        newagg.push(n);
                        index += 1;
                    } else {
                        let new = prevagg[index].unwrap_int_field() + 1;
                        let n = Field::IntField(new);
                        newagg.push(n);
                        index += 1;
                    }
                }

                //prevagg.remove(index);

                //prevagg.insert(index, n);
            }
            let clone = count.clone();
            self.groups.insert(key, (newagg, clone + 1));
        }
        //if group does not exist
        else {
            let mut newgroup = Vec::new();
            let mut index = 0;
            for p in aggfields {
                if tuple.get_field(p.field).unwrap().is_int() {
                    let newagg = match p.op {
                        common::AggOp::Avg => tuple.get_field(p.field).unwrap().unwrap_int_field(),
                        common::AggOp::Count => 1,
                        common::AggOp::Max => tuple.get_field(p.field).unwrap().unwrap_int_field(),
                        common::AggOp::Min => tuple.get_field(p.field).unwrap().unwrap_int_field(),
                        common::AggOp::Sum => tuple.get_field(p.field).unwrap().unwrap_int_field(),
                    };
                    let n = Field::IntField(newagg);
                    newgroup.push(n);
                    index += 1;
                } else {
                    //max min
                    if p.op.max_min() {
                        let newagg = match p.op {
                            common::AggOp::Max => {
                                tuple.get_field(p.field).unwrap().unwrap_string_field()
                            }
                            common::AggOp::Min => {
                                tuple.get_field(p.field).unwrap().unwrap_string_field()
                            }
                            _ => panic!(),
                        };
                        let n = Field::StringField(newagg.to_string());
                        newgroup.push(n);
                        index += 1;
                    }
                    //count
                    else {
                        let newagg = 1;
                        let n = Field::IntField(newagg);
                        newgroup.push(n);
                        index += 1;
                    }
                }
            }
            self.groups.insert(key, (newgroup, 1));
        }
    }

    /// Returns a `TupleIterator` over the results.
    ///
    /// Resulting tuples must be of the form: (group by fields ..., aggregate fields ...)
    pub fn iterator(&self) -> TupleIterator {
        let mut tuples = Vec::new();
        for (group, agg_values) in &self.groups {
            let mut tuple_fields = Vec::new();
            for i in group {
                let field = i.clone();
                tuple_fields.push(field);
            }
            /* for (agg_field) in &agg_values.0 {

                tuple_fields.push(agg_field.clone());
            } */
            for i in 0..agg_values.0.len() {
                let peep = &agg_values.0[i];
                match self.agg_fields[i].op {
                    common::AggOp::Avg => {
                        let temp = peep.unwrap_int_field();
                        tuple_fields.push(Field::IntField(temp / agg_values.1));
                    }
                    _ => {
                        tuple_fields.push(peep.clone());
                    }
                }
            }
            let tuple = Tuple::new(tuple_fields);
            tuples.push(tuple);
        }
        TupleIterator::new(tuples, self.schema.clone())
    }
}

/// Aggregate operator. (You can add any other fields that you think are neccessary)
pub struct Aggregate {
    /// Fields to groupby over.
    groupby_fields: Vec<usize>,
    /// Aggregation fields and corresponding aggregation functions.
    agg_fields: Vec<AggregateField>,
    /// Aggregation iterators for results.
    agg_iter: Option<TupleIterator>,
    /// Output schema of the form [groupby_field attributes ..., agg_field attributes ...]).
    schema: TableSchema,
    /// Boolean if the iterator is open.
    open: bool,
    /// Child operator to get the data from.
    child: Box<dyn OpIterator>,
}

impl Aggregate {
    /// Aggregate constructor.
    ///
    /// # Arguments
    ///
    /// * `groupby_indices` - the indices of the group by fields
    /// * `groupby_names` - the names of the group_by fields in the final aggregation
    /// * `agg_indices` - the indices of the aggregate fields
    /// * `agg_names` - the names of the aggreagte fields in the final aggregation
    /// * `ops` - Aggregate operations, 1:1 correspondence with the indices in agg_indices
    /// * `child` - child operator to get the input data from.
    pub fn new(
        groupby_indices: Vec<usize>,
        groupby_names: Vec<&str>,
        agg_indices: Vec<usize>,
        agg_names: Vec<&str>,
        ops: Vec<AggOp>,
        child: Box<dyn OpIterator>,
    ) -> Self {
        let opsclone = ops.clone();

        let input_schema = child.get_schema();
        let groupby_fields = groupby_indices.clone();
        let aggcopy = agg_indices.clone();
        let aggcopycopy = agg_indices.clone();
        let agg_fields = agg_indices
            .into_iter()
            .zip(ops.clone().into_iter())
            .map(|(field, op)| AggregateField { field, op })
            .collect();
        let agg_fieldscopy = aggcopycopy
            .into_iter()
            .zip(ops.into_iter())
            .map(|(field, op)| AggregateField { field, op })
            .collect();
        let groupby_attributes: Vec<Attribute> = groupby_indices
            .iter()
            .zip(groupby_names.iter())
            .map(|(index, name)| {
                let attribute = input_schema.get_attribute(*index).unwrap().clone();

                Attribute::new(name.to_string(), attribute.dtype().clone())
            })
            .collect();

        /* let agg_attributes: Vec<Attribute> = aggcopy
        .iter()
        .zip(agg_names.iter())
        .map(|(index, name)| {
            let attribute = input_schema.get_attribute(*index).unwrap().clone();

            Attribute::new(name.to_string(), attribute.dtype().clone())
        })
        .collect(); */

        let mut agg_attributes: Vec<Attribute> = Vec::new();
        for (index, i) in agg_names.into_iter().enumerate() {
            let newattribute = match opsclone[index] {
                AggOp::Count => Attribute::new(i.to_string(), common::DataType::Int),
                AggOp::Max => Attribute::new(
                    i.to_string(),
                    input_schema
                        .get_attribute(index)
                        .unwrap()
                        .clone()
                        .dtype()
                        .clone(),
                ),
                AggOp::Min => Attribute::new(
                    i.to_string(),
                    input_schema
                        .get_attribute(index)
                        .unwrap()
                        .clone()
                        .dtype()
                        .clone(),
                ),
                AggOp::Sum => Attribute::new(i.to_string(), common::DataType::Int),
                AggOp::Avg => Attribute::new(i.to_string(), common::DataType::Int),
            };
            agg_attributes.push(newattribute);
        }

        let schema = TableSchema::new([&groupby_attributes[..], &agg_attributes[..]].concat());
        let mut newagg = Aggregator::new(agg_fieldscopy, groupby_fields.clone(), &schema);
        let mut childcopy = child;
        childcopy.open();
        loop {
            match childcopy.next().unwrap() {
                Some(t) => newagg.merge_tuple_into_group(&t),
                None => break,
            }
        }
        /* let mut p = newagg.iterator();
        p.open();
        p.next(); */
        Self {
            groupby_fields,
            agg_fields,
            agg_iter: Some(newagg.iterator()),
            schema,
            open: false,
            child: childcopy,
        }
    }
}

impl OpIterator for Aggregate {
    fn open(&mut self) -> Result<(), CrustyError> {
        self.open = true;
        self.child.open();
        match &mut self.agg_iter {
            Some(iterator) => iterator.open(),
            None => Err(CrustyError::CrustyError("Cannot open".to_string())),
        }
    }

    fn next(&mut self) -> Result<Option<Tuple>, CrustyError> {
        if !self.open {
            panic!("Operator has not been opened")
        }

        match &mut self.agg_iter {
            Some(iterator) => iterator.next(),
            None => Ok(None),
        }
    }

    fn close(&mut self) -> Result<(), CrustyError> {
        if !self.open {
            panic!("Operator has not been opened")
        }

        self.child.close()?;
        self.open = false;
        match &mut self.agg_iter {
            Some(iterator) => iterator.close(),
            None => Err(CrustyError::CrustyError("Cannot open".to_string())),
        }
    }

    fn rewind(&mut self) -> Result<(), CrustyError> {
        if !self.open {
            panic!("Operator has not been opened")
        }
        self.child.rewind()?;
        self.close()?;
        self.open()
    }

    fn get_schema(&self) -> &TableSchema {
        &self.schema
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::opiterator::testutil::*;

    /// Creates a vector of tuples to create the following table:
    ///
    /// 1 1 3 E
    /// 2 1 3 G
    /// 3 1 4 A
    /// 4 2 4 G
    /// 5 2 5 G
    /// 6 2 5 G
    fn tuples() -> Vec<Tuple> {
        let tuples = vec![
            Tuple::new(vec![
                Field::IntField(1),
                Field::IntField(1),
                Field::IntField(3),
                Field::StringField("E".to_string()),
            ]),
            Tuple::new(vec![
                Field::IntField(2),
                Field::IntField(1),
                Field::IntField(3),
                Field::StringField("G".to_string()),
            ]),
            Tuple::new(vec![
                Field::IntField(3),
                Field::IntField(1),
                Field::IntField(4),
                Field::StringField("A".to_string()),
            ]),
            Tuple::new(vec![
                Field::IntField(4),
                Field::IntField(2),
                Field::IntField(4),
                Field::StringField("G".to_string()),
            ]),
            Tuple::new(vec![
                Field::IntField(5),
                Field::IntField(2),
                Field::IntField(5),
                Field::StringField("G".to_string()),
            ]),
            Tuple::new(vec![
                Field::IntField(6),
                Field::IntField(2),
                Field::IntField(5),
                Field::StringField("G".to_string()),
            ]),
        ];
        tuples
    }

    mod aggregator {
        use super::*;
        use common::{DataType, Field};

        /// Set up testing aggregations without grouping.
        ///
        /// # Arguments
        ///
        /// * `op` - Aggregation Operation.
        /// * `field` - Field do aggregation operation over.
        /// * `expected` - The expected result.
        fn test_no_group(op: AggOp, field: usize, expected: i32) -> Result<(), CrustyError> {
            let schema = TableSchema::new(vec![Attribute::new("agg".to_string(), DataType::Int)]);
            let mut agg = Aggregator::new(vec![AggregateField { field, op }], Vec::new(), &schema);
            let ti = tuples();
            for t in &ti {
        
                agg.merge_tuple_into_group(t);
            }

            let mut ai = agg.iterator();
            ai.open()?;
            assert_eq!(
                Field::IntField(expected),
                *ai.next()?.unwrap().get_field(0).unwrap()
            );
            assert_eq!(None, ai.next()?);
            Ok(())
        }

        #[test]
        fn test_merge_tuples_count() -> Result<(), CrustyError> {
            test_no_group(AggOp::Count, 0, 6)
        }

        #[test]
        fn test_merge_tuples_sum() -> Result<(), CrustyError> {
            test_no_group(AggOp::Sum, 1, 9)
        }

        #[test]
        fn test_merge_tuples_max() -> Result<(), CrustyError> {
            test_no_group(AggOp::Max, 0, 6)
        }

        #[test]
        fn test_merge_tuples_min() -> Result<(), CrustyError> {
            test_no_group(AggOp::Min, 0, 1)
        }

        #[test]
        fn test_merge_tuples_avg() -> Result<(), CrustyError> {
            test_no_group(AggOp::Avg, 0, 3)
        }

        #[test]
        #[should_panic]
        fn test_merge_tuples_not_int() {
            let _ = test_no_group(AggOp::Avg, 3, 3);
        }

        #[test]
        fn test_merge_multiple_ops() -> Result<(), CrustyError> {
            let schema = TableSchema::new(vec![
                Attribute::new("agg1".to_string(), DataType::Int),
                Attribute::new("agg2".to_string(), DataType::Int),
            ]);

            let mut agg = Aggregator::new(
                vec![
                    AggregateField {
                        field: 0,
                        op: AggOp::Max,
                    },
                    AggregateField {
                        field: 3,
                        op: AggOp::Count,
                    },
                ],
                Vec::new(),
                &schema,
            );

            let ti = tuples();
            for t in &ti {
                agg.merge_tuple_into_group(t);
            }

            let expected = vec![Field::IntField(6), Field::IntField(6)];
            let mut ai = agg.iterator();
            ai.open()?;
            assert_eq!(Tuple::new(expected), ai.next()?.unwrap());
            Ok(())
        }

        #[test]
        fn test_merge_tuples_one_group() -> Result<(), CrustyError> {
            let schema = TableSchema::new(vec![
                Attribute::new("group".to_string(), DataType::Int),
                Attribute::new("agg".to_string(), DataType::Int),
            ]);
            let mut agg = Aggregator::new(
                vec![AggregateField {
                    field: 0,
                    op: AggOp::Sum,
                }],
                vec![2],
                &schema,
            );

            let ti = tuples();
            for t in &ti {
                agg.merge_tuple_into_group(t);
            }

            let mut ai = agg.iterator();
            ai.open()?;
            let rows = num_tuples(&mut ai)?;
            assert_eq!(3, rows);
            Ok(())
        }

        /// Returns the count of the number of tuples in an OpIterator.
        ///
        /// This function consumes the iterator.
        ///
        /// # Arguments
        ///
        /// * `iter` - Iterator to count.
        pub fn num_tuples(iter: &mut impl OpIterator) -> Result<u32, CrustyError> {
            let mut counter = 0;
            while iter.next()?.is_some() {
                counter += 1;
            }
            Ok(counter)
        }

        #[test]
        fn test_merge_tuples_multiple_groups() -> Result<(), CrustyError> {
            let schema = TableSchema::new(vec![
                Attribute::new("group1".to_string(), DataType::Int),
                Attribute::new("group2".to_string(), DataType::Int),
                Attribute::new("agg".to_string(), DataType::Int),
            ]);

            let mut agg = Aggregator::new(
                vec![AggregateField {
                    field: 0,
                    op: AggOp::Sum,
                }],
                vec![1, 2],
                &schema,
            );

            let ti = tuples();
            for t in &ti {
                agg.merge_tuple_into_group(t);
            }

            let mut ai = agg.iterator();
            ai.open()?;
            let rows = num_tuples(&mut ai)?;
            assert_eq!(4, rows);
            Ok(())
        }
    }

    mod aggregate {
        use super::super::TupleIterator;
        use super::*;
        use common::{DataType, Field};

        fn tuple_iterator() -> TupleIterator {
            let names = vec!["1", "2", "3", "4"];
            let dtypes = vec![
                DataType::Int,
                DataType::Int,
                DataType::Int,
                DataType::String,
            ];
            let schema = TableSchema::from_vecs(names, dtypes);
            let tuples = tuples();
            TupleIterator::new(tuples, schema)
        }

        #[test]
        fn test_open() -> Result<(), CrustyError> {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                Vec::new(),
                Vec::new(),
                vec![0],
                vec!["count"],
                vec![AggOp::Count],
                Box::new(ti),
            );
            assert!(!ai.open);
            ai.open()?;
            assert!(ai.open);
            Ok(())
        }

        fn test_single_agg_no_group(
            op: AggOp,
            op_name: &str,
            col: usize,
            expected: Field,
        ) -> Result<(), CrustyError> {
            
            let ti = tuple_iterator();
            
            let mut ai = Aggregate::new(
                Vec::new(),
                Vec::new(),
                vec![col],
                vec![op_name],
                vec![op],
                Box::new(ti),
            );
            
            ai.open()?;
            //added
            
            assert_eq!(
                // Field::IntField(expected),
                expected,
                *ai.next()?
                    .expect("this one")
                    .get_field(0)
                    .expect("nah this one")
            );
        
            assert_eq!(None, ai.next()?);
            Ok(())
        }

        #[test]
        fn test_single_agg() -> Result<(), CrustyError> {
           
            test_single_agg_no_group(AggOp::Count, "count", 0, Field::IntField(6))?;
          
            test_single_agg_no_group(AggOp::Sum, "sum", 0, Field::IntField(21))?;
       
            test_single_agg_no_group(AggOp::Max, "max", 0, Field::IntField(6))?;

            test_single_agg_no_group(AggOp::Min, "min", 0, Field::IntField(1))?;
  
            test_single_agg_no_group(AggOp::Avg, "avg", 0, Field::IntField(3))?;
    
            test_single_agg_no_group(AggOp::Count, "count", 3, Field::IntField(6))?;
            test_single_agg_no_group(AggOp::Max, "max", 3, Field::StringField("G".to_string()))?; 
            test_single_agg_no_group(AggOp::Min, "min", 3, Field::StringField("A".to_string()))
        }

        #[test]
        fn test_multiple_aggs() -> Result<(), CrustyError> {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                Vec::new(),
                Vec::new(),
                vec![3, 0, 0],
                vec!["count", "avg", "max"],
                vec![AggOp::Count, AggOp::Avg, AggOp::Max],
                Box::new(ti),
            );
            ai.open()?;
            let first_row: Vec<Field> = ai.next()?.unwrap().field_vals().cloned().collect();
            assert_eq!(
                vec![Field::IntField(6), Field::IntField(3), Field::IntField(6)],
                first_row
            );
            ai.close()
        }

        /// Consumes an OpIterator and returns a corresponding 2D Vec of fields
        pub fn iter_to_vec(iter: &mut impl OpIterator) -> Result<Vec<Vec<Field>>, CrustyError> {
            let mut rows = Vec::new();
            iter.open()?;
            while let Some(t) = iter.next()? {
                rows.push(t.field_vals().cloned().collect());
            }
            iter.close()?;
            Ok(rows)
        }

        #[test]
        fn test_multiple_aggs_groups() -> Result<(), CrustyError> {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                vec![1, 2],
                vec!["group1", "group2"],
                vec![3, 0],
                vec!["count", "max"],
                vec![AggOp::Count, AggOp::Max],
                Box::new(ti),
            );
            let mut result = iter_to_vec(&mut ai)?;
            result.sort();
            let expected = vec![
                vec![
                    Field::IntField(1),
                    Field::IntField(3),
                    Field::IntField(2),
                    Field::IntField(2),
                ],
                vec![
                    Field::IntField(1),
                    Field::IntField(4),
                    Field::IntField(1),
                    Field::IntField(3),
                ],
                vec![
                    Field::IntField(2),
                    Field::IntField(4),
                    Field::IntField(1),
                    Field::IntField(4),
                ],
                vec![
                    Field::IntField(2),
                    Field::IntField(5),
                    Field::IntField(2),
                    Field::IntField(6),
                ],
            ];
            assert_eq!(expected, result);
            ai.open()?;
            let num_rows = num_tuples(&mut ai)?;
            ai.close()?;
            assert_eq!(4, num_rows);
            Ok(())
        }

        #[test]
        #[should_panic]
        fn test_next_not_open() {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                Vec::new(),
                Vec::new(),
                vec![0],
                vec!["count"],
                vec![AggOp::Count],
                Box::new(ti),
            );
            ai.next().unwrap();
        }

        #[test]
        fn test_close() -> Result<(), CrustyError> {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                Vec::new(),
                Vec::new(),
                vec![0],
                vec!["count"],
                vec![AggOp::Count],
                Box::new(ti),
            );
            ai.open()?;
            assert!(ai.open);
            ai.close()?;
            assert!(!ai.open);
            Ok(())
        }

        #[test]
        #[should_panic]
        fn test_close_not_open() {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                Vec::new(),
                Vec::new(),
                vec![0],
                vec!["count"],
                vec![AggOp::Count],
                Box::new(ti),
            );
            ai.close().unwrap();
        }

        #[test]
        #[should_panic]
        fn test_rewind_not_open() {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                Vec::new(),
                Vec::new(),
                vec![0],
                vec!["count"],
                vec![AggOp::Count],
                Box::new(ti),
            );
            ai.rewind().unwrap();
        }

        #[test]
        fn test_rewind() -> Result<(), CrustyError> {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                vec![2],
                vec!["group"],
                vec![3],
                vec!["count"],
                vec![AggOp::Count],
                Box::new(ti),
            );
            ai.open()?;
            let count_before = num_tuples(&mut ai);
            ai.rewind()?;
            let count_after = num_tuples(&mut ai);
            ai.close()?;
            assert_eq!(count_before, count_after);
            Ok(())
        }

        #[test]
        fn test_get_schema() {
            let mut agg_names = vec!["count", "max"];
            let mut groupby_names = vec!["group1", "group2"];
            let ti = tuple_iterator();
            let ai = Aggregate::new(
                vec![1, 2],
                groupby_names.clone(),
                vec![3, 0],
                agg_names.clone(),
                vec![AggOp::Count, AggOp::Max],
                Box::new(ti),
            );
            groupby_names.append(&mut agg_names);
            let expected_names = groupby_names;
            let schema = ai.get_schema();
            for (i, attr) in schema.attributes().enumerate() {
                
                assert_eq!(expected_names[i], attr.name());
                assert_eq!(DataType::Int, *attr.dtype());
            }
        }
    }
}
