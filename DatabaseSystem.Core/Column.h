#pragma once

enum class ColumnType
{
	INT32,
	INT64,
	CHAR,
	FLOAT,
	DOUBLE,
};

struct Column
{
public:
	string Name;
	ColumnType Type;
	unsigned int ArraySize;

	Column(string name, ColumnType type, unsigned int arraySize);
	unsigned int getLength();


	static int Compare(Column column, span<unsigned char> a, span<unsigned char> b);
	static bool Equals(Column column, span<unsigned char> a, span<unsigned char> b);
};