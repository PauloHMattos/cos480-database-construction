#pragma once
#include <iostream>

template <class T>
class DoubleList {
public:
	DoubleList()
	{
		// allocate space for the header
		list = new Header;
		if (list == NULL)
		{
			cout << "ERROR : Not enough memory. EXIT !!!\n";
			exit(1);
		}

		// set up the initial values for an empty list
		list->first = new Node;
		if (list->first == NULL)
		{
			cout << "ERROR : Not enough memory. EXIT !!!\n";
			exit(1);
		}

		list->last = new Node;
		if (list->last == NULL)
		{
			cout << "ERROR : Not enough memory. EXIT !!!\n";
			exit(1);
		}

		list->current = list->first;
		list->leftSize = 0;
		list->rightSize = 0;
		list->first->next = list->last;
		list->last->previous = list->first;
	}

	DoubleList(DoubleList& me)
	{
		list = new Header;
		if (list == NULL)
		{
			cout << "ERROR : Not enough memory. EXIT !!!\n";
			exit(1);
		}

		list->first = me.list->first;
		list->last = me.list->last;
		list->current = me.list->current;
		list->leftSize = me.list->leftSize;
		list->rightSize = me.list->rightSize;
	}

	~DoubleList()
	{
		// remove everything
		MoveToStart();
		while (RightLength() > 0) {
			Remove();
		}

		// kill all the nodes
		for (int i = 0; i <= list->leftSize + list->rightSize; i++) {
			list->first = list->first->next;
			delete list->first->previous;
		}
		delete list->first;

		// kill the header
		delete list;
	}

	// swap operator 
	void operator &= (DoubleList& List)
	{
		Header* temp = List.list;
		List.list = list;
		list = temp;
	}

	// add to current pointer position
	void Insert(T* Item)
	{
		Node* temp = new Node;
		if (temp == NULL)
		{
			cout << "ERROR : Not enough memory. EXIT !!!\n";
			exit(1);
		}

		Node* left = list->current;
		Node* right = list->current->next;

		left->next = temp;
		temp->previous = left;
		temp->next = right;
		temp->data = Item;
		if (temp->data == NULL)
		{
			cout << "ERROR : Not enough memory. EXIT !!!\n";
			exit(1);
		}

		right->previous = temp;

		list->rightSize += 1;
	}

	// remove from current position
	void Remove()
	{
		Node* temp = list->current->next;
		list->current->next = temp->next;
		temp->next->previous = list->current;


		delete temp;

		(list->rightSize)--;
	}

	// get a reference to the current item, plus the offset given
	T* Current(int offset)
	{
		Node* temp = list->current->next;
		for (int i = 0; i < offset; i++) {
			temp = temp->next;
		}
		return temp->data;
	}

	// move the current pointer position backward through the list
	void Retreat()
	{
		(list->rightSize)++;
		(list->leftSize)--;
		list->current = list->current->previous;
	}

	// move the current pointer position forward through the list
	void Advance()
	{
		(list->rightSize)--;
		(list->leftSize)++;
		list->current = list->current->next;
	}

	// operations to check the size of both sides
	int LeftLength()
	{
		return (list->leftSize);
	}
	int RightLength()
	{
		return (list->rightSize);
	}

	// operations to swap the left and right sides of two lists
	void SwapLefts(DoubleList& List)
	{
		// swap out everything after the current nodes
		Node* left_1 = list->current;
		Node* right_1 = list->current->next;
		Node* left_2 = List.list->current;
		Node* right_2 = List.list->current->next;

		left_1->next = right_2;
		right_2->previous = left_1;
		left_2->next = right_1;
		right_1->previous = left_2;

		// set the new frontpoints
		Node* temp = list->first;
		list->first = List.list->first;
		List.list->first = temp;

		// set the new current nodes
		temp = list->current;
		list->current = List.list->current;
		List.list->current = temp;

		int tempint = List.list->leftSize;
		List.list->leftSize = list->leftSize;
		list->leftSize = tempint;
	}

	void SwapRights(DoubleList& List)
	{
		// swap out everything after the current nodes
		Node* left_1 = list->current;
		Node* right_1 = list->current->next;
		Node* left_2 = List.list->current;
		Node* right_2 = List.list->current->next;

		left_1->next = right_2;
		right_2->previous = left_1;
		left_2->next = right_1;
		right_1->previous = left_2;

		// set the new endpoints
		Node* temp = list->last;
		list->last = List.list->last;
		List.list->last = temp;

		int tempint = List.list->rightSize;
		List.list->rightSize = list->rightSize;
		list->rightSize = tempint;
	}

	// operations to move the the start of end of a list
	void MoveToStart()
	{
		list->current = list->first;
		list->rightSize += list->leftSize;
		list->leftSize = 0;
	}

	void MoveToFinish()
	{
		list->current = list->last->previous;
		list->leftSize += list->rightSize;
		list->rightSize = 0;
	}

private:

	struct Node {
		// data
		T* data;
		Node* next;
		Node* previous;

		// constructor
		Node() : data(0), next(0), previous(0) {}

		// deconstructor 
		~Node()
		{
			delete data;
		}
	};


	struct Header {
		// data
		Node* first;
		Node* last;
		Node* current;
		int leftSize;
		int rightSize;
	};

	// the list itself is pointed to by this pointer	
	Header* list;

};

