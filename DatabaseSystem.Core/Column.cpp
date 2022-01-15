#include "pch.h"
#include "Column.h"

Column::Column(string name, ColumnType type, unsigned int arraySize) : Name(name), Type(type), ArraySize(arraySize)
{
}

unsigned int Column::GetLength() const
{
	switch (Type)
	{
	case ColumnType::INT32:
	case ColumnType::FLOAT:
		return sizeof(int) * ArraySize;

	case ColumnType::INT64:
	case ColumnType::DOUBLE:
		return sizeof(long long) * ArraySize;

	case ColumnType::CHAR:
		return sizeof(char) * ArraySize;

	case ColumnType::VARCHAR:
	default:
		return 0;
	}
	return 0;
}

int Column::Compare(const Column& column, span<unsigned char> a, span<unsigned char> b)
{
	if (a.size() != b.size() || b.size() != column.GetLength())
	{
		throw runtime_error("a.size() != b.size()");
	}

	switch (column.Type)
	{
	case ColumnType::INT32:
	{
		auto valueA = *(int*)a.data();
		auto valueB = *(int*)b.data();
		return valueA == valueB ? 0 : (valueA > valueB ? 1 : -1);
	}
	
	case ColumnType::INT64:
	{
		auto valueA = *(long long*)a.data();
		auto valueB = *(long long*)b.data();
		return valueA == valueB ? 0 : (valueA > valueB ? 1 : -1);
	}
	
	case ColumnType::DOUBLE:
	{
		auto valueA = *(double*)a.data();
		auto valueB = *(double*)b.data();
		return valueA == valueB ? 0 : (valueA > valueB ? 1 : -1);
	}
	
	case ColumnType::FLOAT:
	{
		auto valueA = *(float*)a.data();
		auto valueB = *(float*)b.data();
		return valueA == valueB ? 0 : (valueA > valueB ? 1 : -1);
	}

	case ColumnType::CHAR:
		return memcmp(a.data(), b.data(), a.size());

	default:
		throw runtime_error("Type not implemented");
	}
	return 0;
}

bool Column::Equals(const Column& column, span<unsigned char> a, span<unsigned char> b)
{
	if (a.size() != b.size() || b.size() != column.GetLength())
	{
		throw runtime_error("a.size() != b.size()");
	}

	return memcmp(a.data(), b.data(), a.size()) == 0;
}


void Column::Parse(const Column& column, span<unsigned char> destination, string str)
{
	if (destination.size() != column.GetLength())
	{
		throw runtime_error("destination.size() != column.getLength()");
	}

	switch (column.Type)
	{
		case ColumnType::INT32:
		{
			auto value = stoi(str);
			memcpy(destination.data(), &value, sizeof(int));
			break;
		}

		case ColumnType::INT64: 
		{
			auto value = stoll(str);
			memcpy(destination.data(), &value, sizeof(long long));
			break;
		}

		case ColumnType::DOUBLE:
		{
			auto value = stod(str);
			memcpy(destination.data(), &value, sizeof(double));
			break;
		}

		case ColumnType::FLOAT:
		{
			auto value = stof(str);
			memcpy(destination.data(), &value, sizeof(float));
			break;
		}

		case ColumnType::CHAR:
		{
			auto value = str.c_str();
			auto length = min(str.length(), destination.size());
			memcpy(destination.data(), value, length);
			break;
		}

		default:
		{
			throw runtime_error("Type not implemented");
		}
	}
}

void Column::WriteValue(ostream& out, const Column& column, span<unsigned char> value) 
{

	if (value.size() != column.GetLength())
	{
		throw runtime_error("value.size() != column.getLength()");
	}

	switch (column.Type)
	{
	case ColumnType::INT32:
		out << *(int*)value.data();
		break;

	case ColumnType::INT64:
		out << *(long long*)value.data();
		break;

	case ColumnType::DOUBLE:
		out << *(double*)value.data();
		break;

	case ColumnType::FLOAT:
		out << *(float*)value.data();
		break;

	case ColumnType::CHAR:
		out.write((const char*)value.data(), value.size());
		break;

	default:
		throw runtime_error("Type not implemented");
	}
}