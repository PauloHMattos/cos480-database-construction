#pragma once
#include "Column.h"
#include "Serializeble.h"

class Schema : public Serializable
{
public:
	Schema();
	void AddColumn(string name, ColumnType type, unsigned int arraySize = 1);
	unsigned int GetSize() const;
	Column GetColumn(unsigned int columnId);
	unsigned int GetColumnId(string columnName) const;
	span<unsigned char> GetValue(vector<unsigned char>* data, unsigned int columnId);

	void Write(ostream& out, vector<unsigned char>* data);

	// Var length
	bool IsVarLengthColumn(ColumnType type);
	unsigned int GetVarSize() const;

	// Inherited via Serializable
	void Serialize(iostream& dst) override;
	void Deserialize(iostream& src) override;
private:
	vector<Column> m_Columns;
	unsigned int m_Size;
	unsigned int m_VarLengthColumns;
	vector<unsigned int> m_FixedLengthColumnPos;
	vector<unsigned int> m_VarLengthColumnPos;
};
