use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LabelId(pub u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RelTypeId(pub u32);

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

#[derive(Debug, Default, Clone)]
pub struct Catalog {
    labels_by_name: BTreeMap<String, LabelId>,
    labels: Vec<Label>,
    rel_types_by_name: BTreeMap<String, RelTypeId>,
    rel_types: Vec<RelType>,
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
