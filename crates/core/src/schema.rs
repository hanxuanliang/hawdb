use crate::value::Value;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LabelId(pub u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RelTypeId(pub u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TableId(pub u32);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Label {
    pub id: LabelId,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelType {
    pub id: RelTypeId,
    pub name: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct IndexId(pub u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ConstraintId(pub u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PropertyId(pub u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum IndexKind {
    Equality,
    Range,
    FullText,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ConstraintKind {
    NodePropertyUnique,
    NodePropertyExists,
    RelationshipPropertyUnique,
    RelationshipPropertyExists,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ConstraintSubject {
    Node(LabelId),
    Relationship(RelTypeId),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TableKind {
    Node,
    Relationship,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaObjectState {
    DeleteOnly,
    WriteOnly,
    Backfill,
    Validating,
    Public,
    Gc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PropertyType {
    Any,
    Bool,
    Int,
    Float,
    String,
    List,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexDescriptor {
    pub id: IndexId,
    pub label_id: LabelId,
    pub property: String,
    pub kind: IndexKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompositeIndexDescriptor {
    pub id: IndexId,
    pub label_id: LabelId,
    pub properties: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConstraintDescriptor {
    pub id: ConstraintId,
    pub subject: ConstraintSubject,
    pub property: String,
    pub kind: ConstraintKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableDescriptor {
    pub id: TableId,
    pub name: String,
    pub kind: TableKind,
    pub state: SchemaObjectState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PropertyDescriptor {
    pub id: PropertyId,
    pub table_id: TableId,
    pub name: String,
    pub value_type: PropertyType,
    pub nullable: bool,
    pub state: SchemaObjectState,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct GraphStatistics {
    pub computed_at_commit_epoch: u64,
    pub histogram_sample_limit: usize,
    pub node_count: u64,
    pub relationship_count: u64,
    pub label_counts: BTreeMap<LabelId, u64>,
    pub rel_type_counts: BTreeMap<RelTypeId, u64>,
    pub rel_type_source_counts: BTreeMap<RelTypeId, u64>,
    pub path_counts: BTreeMap<(LabelId, RelTypeId, LabelId), u64>,
    pub bounded_path_counts: BTreeMap<(LabelId, RelTypeId, LabelId, usize), u64>,
    pub property_distinct_counts: BTreeMap<(LabelId, String), u64>,
    pub rel_property_distinct_counts: BTreeMap<(RelTypeId, String), u64>,
    pub property_histograms: BTreeMap<(LabelId, String), Vec<Value>>,
    pub rel_property_histograms: BTreeMap<(RelTypeId, String), Vec<Value>>,
    pub sampled_property_histograms: BTreeMap<(LabelId, String), bool>,
}

#[derive(Debug, Default, Clone)]
pub struct Catalog {
    labels_by_name: BTreeMap<String, LabelId>,
    labels: Vec<Label>,
    rel_types_by_name: BTreeMap<String, RelTypeId>,
    rel_types: Vec<RelType>,
    tables_by_key: BTreeMap<(TableKind, String), TableId>,
    tables: Vec<TableDescriptor>,
    properties_by_key: BTreeMap<(TableId, String), PropertyId>,
    properties: Vec<PropertyDescriptor>,
    property_indexes_by_key: BTreeMap<(LabelId, String, IndexKind), IndexId>,
    property_indexes: Vec<IndexDescriptor>,
    composite_property_indexes_by_key: BTreeMap<(LabelId, Vec<String>), IndexId>,
    composite_property_indexes: Vec<CompositeIndexDescriptor>,
    constraints_by_key: BTreeMap<(ConstraintSubject, String, ConstraintKind), ConstraintId>,
    constraints: Vec<ConstraintDescriptor>,
}

impl Catalog {
    pub fn get_or_create_label(&mut self, name: &str) -> LabelId {
        if let Some(id) = self.labels_by_name.get(name) {
            return *id;
        }
        let id = LabelId(self.labels.len() as u32);
        self.labels.push(Label {
            id,
            name: name.to_string(),
        });
        self.labels_by_name.insert(name.to_string(), id);
        id
    }

    pub fn label_id(&self, name: &str) -> Option<LabelId> {
        self.labels_by_name.get(name).copied()
    }

    pub fn label_name(&self, id: LabelId) -> Option<&str> {
        self.labels
            .get(id.0 as usize)
            .map(|label| label.name.as_str())
    }

    pub fn labels(&self) -> impl Iterator<Item = &Label> {
        self.labels.iter()
    }

    pub fn get_or_create_rel_type(&mut self, name: &str) -> RelTypeId {
        if let Some(id) = self.rel_types_by_name.get(name) {
            return *id;
        }
        let id = RelTypeId(self.rel_types.len() as u32);
        self.rel_types.push(RelType {
            id,
            name: name.to_string(),
        });
        self.rel_types_by_name.insert(name.to_string(), id);
        id
    }

    pub fn rel_type_id(&self, name: &str) -> Option<RelTypeId> {
        self.rel_types_by_name.get(name).copied()
    }

    pub fn rel_type_name(&self, id: RelTypeId) -> Option<&str> {
        self.rel_types
            .get(id.0 as usize)
            .map(|rel_type| rel_type.name.as_str())
    }

    pub fn rel_types(&self) -> impl Iterator<Item = &RelType> {
        self.rel_types.iter()
    }

    pub fn get_or_create_table(&mut self, kind: TableKind, name: &str) -> TableId {
        let key = (kind, name.to_string());
        if let Some(id) = self.tables_by_key.get(&key) {
            return *id;
        }
        let id = TableId(self.tables.len() as u32);
        self.tables.push(TableDescriptor {
            id,
            name: name.to_string(),
            kind,
            state: SchemaObjectState::Public,
        });
        self.tables_by_key.insert(key, id);
        id
    }

    pub fn table_id(&self, kind: TableKind, name: &str) -> Option<TableId> {
        self.tables_by_key.get(&(kind, name.to_string())).copied()
    }

    pub fn table_descriptor(&self, id: TableId) -> Option<&TableDescriptor> {
        self.tables
            .get(id.0 as usize)
            .filter(|table| !table.name.is_empty())
    }

    pub fn set_table_state(&mut self, id: TableId, state: SchemaObjectState) -> bool {
        let Some(table) = self.tables.get_mut(id.0 as usize) else {
            return false;
        };
        if table.name.is_empty() {
            return false;
        }
        table.state = state;
        true
    }

    pub fn remove_table_descriptor(&mut self, id: TableId) -> bool {
        let Some(table) = self.tables.get_mut(id.0 as usize) else {
            return false;
        };
        if table.name.is_empty() {
            return false;
        }
        let key = (table.kind, table.name.clone());
        table.name.clear();
        self.tables_by_key.remove(&key);

        let property_ids = self
            .properties
            .iter()
            .filter(|property| property.table_id == id && !property.name.is_empty())
            .map(|property| property.id)
            .collect::<Vec<_>>();
        for property_id in property_ids {
            self.remove_property_descriptor(property_id);
        }
        true
    }

    pub fn import_table(
        &mut self,
        id: TableId,
        kind: TableKind,
        name: String,
        state: SchemaObjectState,
    ) {
        let index = id.0 as usize;
        while self.tables.len() <= index {
            let next = TableId(self.tables.len() as u32);
            self.tables.push(TableDescriptor {
                id: next,
                name: String::new(),
                kind: TableKind::Node,
                state: SchemaObjectState::Public,
            });
        }
        self.tables[index] = TableDescriptor {
            id,
            name: name.clone(),
            kind,
            state,
        };
        self.tables_by_key.insert((kind, name), id);
    }

    pub fn table_descriptors(&self) -> impl Iterator<Item = &TableDescriptor> {
        self.tables.iter().filter(|table| !table.name.is_empty())
    }

    pub fn get_or_create_property(
        &mut self,
        table_id: TableId,
        name: &str,
        value_type: PropertyType,
        nullable: bool,
    ) -> PropertyId {
        let key = (table_id, name.to_string());
        if let Some(id) = self.properties_by_key.get(&key) {
            return *id;
        }
        let id = PropertyId(self.properties.len() as u32);
        self.properties.push(PropertyDescriptor {
            id,
            table_id,
            name: name.to_string(),
            value_type,
            nullable,
            state: SchemaObjectState::Public,
        });
        self.properties_by_key.insert(key, id);
        id
    }

    pub fn property_descriptor_id(&self, table_id: TableId, name: &str) -> Option<PropertyId> {
        self.properties_by_key
            .get(&(table_id, name.to_string()))
            .copied()
    }

    pub fn import_property_descriptor(
        &mut self,
        id: PropertyId,
        table_id: TableId,
        name: String,
        value_type: PropertyType,
        nullable: bool,
        state: SchemaObjectState,
    ) {
        let index = id.0 as usize;
        while self.properties.len() <= index {
            let next = PropertyId(self.properties.len() as u32);
            self.properties.push(PropertyDescriptor {
                id: next,
                table_id: TableId(0),
                name: String::new(),
                value_type: PropertyType::Any,
                nullable: true,
                state: SchemaObjectState::Public,
            });
        }
        self.properties[index] = PropertyDescriptor {
            id,
            table_id,
            name: name.clone(),
            value_type,
            nullable,
            state,
        };
        self.properties_by_key.insert((table_id, name), id);
    }

    pub fn property_descriptors(&self) -> impl Iterator<Item = &PropertyDescriptor> {
        self.properties
            .iter()
            .filter(|property| !property.name.is_empty())
    }

    pub fn property_descriptor(&self, id: PropertyId) -> Option<&PropertyDescriptor> {
        self.properties
            .get(id.0 as usize)
            .filter(|property| !property.name.is_empty())
    }

    pub fn set_property_state(&mut self, id: PropertyId, state: SchemaObjectState) -> bool {
        let Some(property) = self.properties.get_mut(id.0 as usize) else {
            return false;
        };
        if property.name.is_empty() {
            return false;
        }
        property.state = state;
        true
    }

    pub fn remove_property_descriptor(&mut self, id: PropertyId) -> bool {
        let Some(property) = self.properties.get_mut(id.0 as usize) else {
            return false;
        };
        if property.name.is_empty() {
            return false;
        }
        let key = (property.table_id, property.name.clone());
        property.name.clear();
        self.properties_by_key.remove(&key);
        true
    }

    pub fn get_or_create_property_index(&mut self, label_id: LabelId, property: &str) -> IndexId {
        self.get_or_create_property_index_with_kind(label_id, property, IndexKind::Equality)
    }

    pub fn get_or_create_property_index_with_kind(
        &mut self,
        label_id: LabelId,
        property: &str,
        kind: IndexKind,
    ) -> IndexId {
        let key = (label_id, property.to_string());
        let key = (key.0, key.1, kind);
        if let Some(id) = self.property_indexes_by_key.get(&key) {
            return *id;
        }
        let id = IndexId(self.property_indexes.len() as u32);
        self.property_indexes.push(IndexDescriptor {
            id,
            label_id,
            property: property.to_string(),
            kind,
        });
        self.property_indexes_by_key.insert(key, id);
        id
    }

    pub fn property_index_id(&self, label_id: LabelId, property: &str) -> Option<IndexId> {
        self.property_index_id_with_kind(label_id, property, IndexKind::Equality)
    }

    pub fn property_index_id_with_kind(
        &self,
        label_id: LabelId,
        property: &str,
        kind: IndexKind,
    ) -> Option<IndexId> {
        self.property_indexes_by_key
            .get(&(label_id, property.to_string(), kind))
            .copied()
    }

    pub fn import_property_index(&mut self, id: IndexId, label_id: LabelId, property: String) {
        self.import_property_index_with_kind(id, label_id, property, IndexKind::Equality);
    }

    pub fn import_property_index_with_kind(
        &mut self,
        id: IndexId,
        label_id: LabelId,
        property: String,
        kind: IndexKind,
    ) {
        let index = id.0 as usize;
        while self.property_indexes.len() <= index {
            let next = IndexId(self.property_indexes.len() as u32);
            self.property_indexes.push(IndexDescriptor {
                id: next,
                label_id: LabelId(0),
                property: String::new(),
                kind: IndexKind::Equality,
            });
        }
        self.property_indexes[index] = IndexDescriptor {
            id,
            label_id,
            property: property.clone(),
            kind,
        };
        self.property_indexes_by_key
            .insert((label_id, property, kind), id);
    }

    pub fn property_indexes(&self) -> impl Iterator<Item = &IndexDescriptor> {
        self.property_indexes
            .iter()
            .filter(|index| !index.property.is_empty())
    }

    pub fn get_or_create_composite_property_index(
        &mut self,
        label_id: LabelId,
        properties: &[String],
    ) -> IndexId {
        let key = (label_id, properties.to_vec());
        if let Some(id) = self.composite_property_indexes_by_key.get(&key) {
            return *id;
        }
        let id =
            IndexId((self.property_indexes.len() + self.composite_property_indexes.len()) as u32);
        self.composite_property_indexes
            .push(CompositeIndexDescriptor {
                id,
                label_id,
                properties: properties.to_vec(),
            });
        self.composite_property_indexes_by_key.insert(key, id);
        id
    }

    pub fn composite_property_index_id(
        &self,
        label_id: LabelId,
        properties: &[String],
    ) -> Option<IndexId> {
        self.composite_property_indexes_by_key
            .get(&(label_id, properties.to_vec()))
            .copied()
    }

    pub fn import_composite_property_index(
        &mut self,
        id: IndexId,
        label_id: LabelId,
        properties: Vec<String>,
    ) {
        self.composite_property_indexes
            .push(CompositeIndexDescriptor {
                id,
                label_id,
                properties: properties.clone(),
            });
        self.composite_property_indexes_by_key
            .insert((label_id, properties), id);
    }

    pub fn composite_property_indexes(&self) -> impl Iterator<Item = &CompositeIndexDescriptor> {
        self.composite_property_indexes
            .iter()
            .filter(|index| !index.properties.is_empty())
    }

    pub fn get_or_create_unique_constraint(
        &mut self,
        label_id: LabelId,
        property: &str,
    ) -> ConstraintId {
        self.get_or_create_constraint(
            ConstraintSubject::Node(label_id),
            property,
            ConstraintKind::NodePropertyUnique,
        )
    }

    pub fn unique_constraint_id(&self, label_id: LabelId, property: &str) -> Option<ConstraintId> {
        self.constraint_id(
            ConstraintSubject::Node(label_id),
            property,
            ConstraintKind::NodePropertyUnique,
        )
    }

    pub fn get_or_create_node_property_exists_constraint(
        &mut self,
        label_id: LabelId,
        property: &str,
    ) -> ConstraintId {
        self.get_or_create_constraint(
            ConstraintSubject::Node(label_id),
            property,
            ConstraintKind::NodePropertyExists,
        )
    }

    pub fn node_property_exists_constraint_id(
        &self,
        label_id: LabelId,
        property: &str,
    ) -> Option<ConstraintId> {
        self.constraint_id(
            ConstraintSubject::Node(label_id),
            property,
            ConstraintKind::NodePropertyExists,
        )
    }

    pub fn get_or_create_relationship_property_exists_constraint(
        &mut self,
        rel_type_id: RelTypeId,
        property: &str,
    ) -> ConstraintId {
        self.get_or_create_constraint(
            ConstraintSubject::Relationship(rel_type_id),
            property,
            ConstraintKind::RelationshipPropertyExists,
        )
    }

    pub fn relationship_property_exists_constraint_id(
        &self,
        rel_type_id: RelTypeId,
        property: &str,
    ) -> Option<ConstraintId> {
        self.constraint_id(
            ConstraintSubject::Relationship(rel_type_id),
            property,
            ConstraintKind::RelationshipPropertyExists,
        )
    }

    pub fn get_or_create_relationship_unique_constraint(
        &mut self,
        rel_type_id: RelTypeId,
        property: &str,
    ) -> ConstraintId {
        self.get_or_create_constraint(
            ConstraintSubject::Relationship(rel_type_id),
            property,
            ConstraintKind::RelationshipPropertyUnique,
        )
    }

    pub fn relationship_unique_constraint_id(
        &self,
        rel_type_id: RelTypeId,
        property: &str,
    ) -> Option<ConstraintId> {
        self.constraint_id(
            ConstraintSubject::Relationship(rel_type_id),
            property,
            ConstraintKind::RelationshipPropertyUnique,
        )
    }

    pub fn import_unique_constraint(
        &mut self,
        id: ConstraintId,
        label_id: LabelId,
        property: String,
    ) {
        self.import_constraint(
            id,
            ConstraintSubject::Node(label_id),
            property,
            ConstraintKind::NodePropertyUnique,
        );
    }

    pub fn import_node_property_exists_constraint(
        &mut self,
        id: ConstraintId,
        label_id: LabelId,
        property: String,
    ) {
        self.import_constraint(
            id,
            ConstraintSubject::Node(label_id),
            property,
            ConstraintKind::NodePropertyExists,
        );
    }

    pub fn import_relationship_property_exists_constraint(
        &mut self,
        id: ConstraintId,
        rel_type_id: RelTypeId,
        property: String,
    ) {
        self.import_constraint(
            id,
            ConstraintSubject::Relationship(rel_type_id),
            property,
            ConstraintKind::RelationshipPropertyExists,
        );
    }

    pub fn import_relationship_unique_constraint(
        &mut self,
        id: ConstraintId,
        rel_type_id: RelTypeId,
        property: String,
    ) {
        self.import_constraint(
            id,
            ConstraintSubject::Relationship(rel_type_id),
            property,
            ConstraintKind::RelationshipPropertyUnique,
        );
    }

    fn get_or_create_constraint(
        &mut self,
        subject: ConstraintSubject,
        property: &str,
        kind: ConstraintKind,
    ) -> ConstraintId {
        let key = (subject, property.to_string(), kind);
        if let Some(id) = self.constraints_by_key.get(&key) {
            return *id;
        }
        let id = ConstraintId(self.constraints.len() as u32);
        self.constraints.push(ConstraintDescriptor {
            id,
            subject,
            property: property.to_string(),
            kind,
        });
        self.constraints_by_key.insert(key, id);
        id
    }

    fn constraint_id(
        &self,
        subject: ConstraintSubject,
        property: &str,
        kind: ConstraintKind,
    ) -> Option<ConstraintId> {
        self.constraints_by_key
            .get(&(subject, property.to_string(), kind))
            .copied()
    }

    fn import_constraint(
        &mut self,
        id: ConstraintId,
        subject: ConstraintSubject,
        property: String,
        kind: ConstraintKind,
    ) {
        let index = id.0 as usize;
        while self.constraints.len() <= index {
            let next = ConstraintId(self.constraints.len() as u32);
            self.constraints.push(ConstraintDescriptor {
                id: next,
                subject,
                property: String::new(),
                kind,
            });
        }
        self.constraints[index] = ConstraintDescriptor {
            id,
            subject,
            property: property.clone(),
            kind,
        };
        self.constraints_by_key
            .insert((subject, property, kind), id);
    }

    pub fn unique_constraints(&self) -> impl Iterator<Item = &ConstraintDescriptor> {
        self.constraints.iter().filter(|constraint| {
            !constraint.property.is_empty() && constraint.kind == ConstraintKind::NodePropertyUnique
        })
    }

    pub fn node_property_exists_constraints(&self) -> impl Iterator<Item = &ConstraintDescriptor> {
        self.constraints.iter().filter(|constraint| {
            !constraint.property.is_empty() && constraint.kind == ConstraintKind::NodePropertyExists
        })
    }

    pub fn relationship_property_exists_constraints(
        &self,
    ) -> impl Iterator<Item = &ConstraintDescriptor> {
        self.constraints.iter().filter(|constraint| {
            !constraint.property.is_empty()
                && constraint.kind == ConstraintKind::RelationshipPropertyExists
        })
    }

    pub fn relationship_unique_constraints(&self) -> impl Iterator<Item = &ConstraintDescriptor> {
        self.constraints.iter().filter(|constraint| {
            !constraint.property.is_empty()
                && constraint.kind == ConstraintKind::RelationshipPropertyUnique
        })
    }

    pub fn import_label(&mut self, id: LabelId, name: String) {
        let index = id.0 as usize;
        while self.labels.len() <= index {
            let next = LabelId(self.labels.len() as u32);
            self.labels.push(Label {
                id: next,
                name: String::new(),
            });
        }
        self.labels[index] = Label {
            id,
            name: name.clone(),
        };
        self.labels_by_name.insert(name, id);
    }

    pub fn import_rel_type(&mut self, id: RelTypeId, name: String) {
        let index = id.0 as usize;
        while self.rel_types.len() <= index {
            let next = RelTypeId(self.rel_types.len() as u32);
            self.rel_types.push(RelType {
                id: next,
                name: String::new(),
            });
        }
        self.rel_types[index] = RelType {
            id,
            name: name.clone(),
        };
        self.rel_types_by_name.insert(name, id);
    }
}
